import io
import json
import os
import re
import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

import mysql.connector
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "showlang"),
    "database": os.getenv("DB_NAME", "dataflow_digram"),
}

DB_SSL_CA = str(os.getenv("DB_SSL_CA", "")).strip()
DB_SSL_DISABLED = str(os.getenv("DB_SSL_DISABLED", "false")).strip().lower() == "true"
DB_SSL_VERIFY_CERT = str(os.getenv("DB_SSL_VERIFY_CERT", "false")).strip().lower() == "true"
DB_SSL_VERIFY_IDENTITY = str(os.getenv("DB_SSL_VERIFY_IDENTITY", "false")).strip().lower() == "true"

if DB_SSL_DISABLED:
    DB_CONFIG["ssl_disabled"] = True
elif DB_SSL_CA:
    DB_CONFIG["ssl_ca"] = DB_SSL_CA
    DB_CONFIG["ssl_verify_cert"] = DB_SSL_VERIFY_CERT
    DB_CONFIG["ssl_verify_identity"] = DB_SSL_VERIFY_IDENTITY

CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://localhost:8088,http://127.0.0.1:8088,http://localhost:5500,http://127.0.0.1:5500,http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,http://127.0.0.1:3000,null",
    ).split(",")
    if origin.strip()
]
CORS_ALLOW_ORIGIN_REGEX = str(os.getenv("CORS_ALLOW_ORIGIN_REGEX", "")).strip() or None

AUTH_COOKIE_NAME = os.getenv("AUTH_COOKIE_NAME", "df_session")
AUTH_COOKIE_SECURE = os.getenv("AUTH_COOKIE_SECURE", "false").strip().lower() == "true"
AUTH_COOKIE_SAMESITE = str(os.getenv("AUTH_COOKIE_SAMESITE", "lax")).strip().lower()
AUTH_COOKIE_DOMAIN = str(os.getenv("AUTH_COOKIE_DOMAIN", "")).strip() or None
AUTH_SESSION_HOURS = int(os.getenv("AUTH_SESSION_HOURS", "12"))
AUTH_PASSWORD_MIN_LEN = int(os.getenv("AUTH_PASSWORD_MIN_LEN", "8"))
AUTH_LOGIN_MAX_FAILS = int(os.getenv("AUTH_LOGIN_MAX_FAILS", "5"))
AUTH_TEMP_LOCK_MINUTES = int(os.getenv("AUTH_TEMP_LOCK_MINUTES", "15"))
DEFAULT_ADMIN_USERNAME = os.getenv("DEFAULT_ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASSWORD = os.getenv("DEFAULT_ADMIN_PASSWORD", "Admin@123456")

if AUTH_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    AUTH_COOKIE_SAMESITE = "lax"

ALLOWED_TABLES = {"rstran", "bw_object_name"}

# Use explicit duplicate-check fields where business rules differ from physical PK design.
DUPLICATE_CHECK_FIELDS: Dict[str, List[str]] = {
    "bw_object_name": ["BW_OBJECT", "SOURCESYS"],
}

# Fields allowed to be empty during import key validation.
NULLABLE_KEY_FIELDS: Dict[str, set[str]] = {
    "bw_object_name": {"SOURCESYS"},
}

app = FastAPI(title="Dataflow Import Status API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_origin_regex=CORS_ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ImportStatusUpdate(BaseModel):
    table_name: str


class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


class AdminCreateUserRequest(BaseModel):
    username: str
    password: str
    role: str


class AdminResetPasswordRequest(BaseModel):
    new_password: str


class HiddenObjectRequest(BaseModel):
    bw_object: str
    sourcesys: str = ""


def utcnow() -> datetime:
    return datetime.utcnow()


def normalize_username(value: str) -> str:
    return str(value or "").strip().lower()


def normalize_bw_object_lookup(value: str | None) -> str:
    return str(value or "").strip().upper()


def validate_password_strength(password: str) -> None:
    raw = str(password or "")
    if len(raw) < AUTH_PASSWORD_MIN_LEN:
        raise HTTPException(status_code=400, detail=f"密码至少 {AUTH_PASSWORD_MIN_LEN} 位")
    if not any(c.islower() for c in raw):
        raise HTTPException(status_code=400, detail="密码必须包含小写字母")
    if not any(c.isupper() for c in raw):
        raise HTTPException(status_code=400, detail="密码必须包含大写字母")
    if not any(c.isdigit() for c in raw):
        raise HTTPException(status_code=400, detail="密码必须包含数字")
    if not any(not c.isalnum() for c in raw):
        raise HTTPException(status_code=400, detail="密码必须包含特殊字符")


def hash_password(password: str, iterations: int = 210000) -> str:
    salt = secrets.token_bytes(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.b64encode(salt).decode("ascii")
    hash_b64 = base64.b64encode(hashed).decode("ascii")
    return f"pbkdf2_sha256${iterations}${salt_b64}${hash_b64}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        algo, iterations_txt, salt_b64, hash_b64 = str(encoded or "").split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iterations_txt)
        salt = base64.b64decode(salt_b64.encode("ascii"))
        expected = base64.b64decode(hash_b64.encode("ascii"))
    except Exception:
        return False

    actual = hashlib.pbkdf2_hmac("sha256", str(password or "").encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def hash_session_token(raw_token: str) -> str:
    return hashlib.sha256(str(raw_token or "").encode("utf-8")).hexdigest()


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def ensure_status_table() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS `import_status` (
          `table_name` VARCHAR(64) NOT NULL,
          `last_import_at` DATETIME NOT NULL,
          `last_import_count` INT NOT NULL DEFAULT 0,
          `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`table_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )

    # Backward compatible migration for existing table.
    cur.execute(
        """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'import_status' AND COLUMN_NAME = 'last_import_count'
        """,
        (DB_CONFIG["database"],),
    )
    has_count_col = cur.fetchone()[0] > 0
    if not has_count_col:
        cur.execute("ALTER TABLE import_status ADD COLUMN last_import_count INT NOT NULL DEFAULT 0 AFTER last_import_at")

    conn.commit()
    cur.close()
    conn.close()


def ensure_bw_object_name_schema() -> None:
    """Keep bw_object_name schema aligned with business rules.

    - SOURCESYS can be NULL.
    - Use NAME_EN / NAME_DE fields.
    - Remove legacy OBJECT_NAME field.
    """
    conn = get_conn()
    cur = conn.cursor()

    def ensure_bw_object_name_indexes() -> None:
        cur.execute("SHOW INDEX FROM `bw_object_name` WHERE Key_name = 'idx_bw_object_lookup'")
        if not cur.fetchall():
            cur.execute(
                "CREATE INDEX `idx_bw_object_lookup` ON `bw_object_name` (`BW_OBJECT`, `BW_OBJECT_TYPE`, `SOURCESYS`)"
            )

        cur.execute("SHOW INDEX FROM `bw_object_name` WHERE Key_name = 'idx_bw_object_sourcesys'")
        if not cur.fetchall():
            cur.execute(
                "CREATE INDEX `idx_bw_object_sourcesys` ON `bw_object_name` (`BW_OBJECT`, `SOURCESYS`)"
            )

        cur.execute("SHOW INDEX FROM `bw_object_name` WHERE Key_name = 'idx_bw_object_norm_lookup'")
        if not cur.fetchall():
            cur.execute(
                "CREATE INDEX `idx_bw_object_norm_lookup` ON `bw_object_name` (`BW_OBJECT_NORM`, `BW_OBJECT_TYPE`, `SOURCESYS`)"
            )

        cur.execute("SHOW INDEX FROM `bw_object_name` WHERE Key_name = 'idx_bw_object_norm_sourcesys'")
        if not cur.fetchall():
            cur.execute(
                "CREATE INDEX `idx_bw_object_norm_sourcesys` ON `bw_object_name` (`BW_OBJECT_NORM`, `SOURCESYS`)"
            )

    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name'
            """,
            (DB_CONFIG["database"],),
        )
        if int(cur.fetchone()[0]) == 0:
            cur.execute(
                """
                CREATE TABLE `bw_object_name` (
                  `BW_OBJECT` varchar(40) NOT NULL COMMENT 'BW object',
                                    `BW_OBJECT_NORM` varchar(40) NOT NULL COMMENT 'BW object normalized to uppercase',
                  `SOURCESYS` varchar(25) NULL COMMENT 'Source System',
                  `BW_OBJECT_TYPE` varchar(20) NULL COMMENT 'BW object type',
                  `NAME_EN` varchar(255) NULL COMMENT 'Object Name (EN)',
                  `NAME_DE` varchar(255) NULL COMMENT 'Object Name (DE)',
                                    `NAME_EN_NORM` varchar(255) NULL COMMENT 'Object Name (EN) normalized to uppercase',
                                    `NAME_DE_NORM` varchar(255) NULL COMMENT 'Object Name (DE) normalized to uppercase',
                  KEY `idx_bw_object_sourcesys` (`BW_OBJECT`, `SOURCESYS`),
                                    KEY `idx_bw_object_lookup` (`BW_OBJECT`, `BW_OBJECT_TYPE`, `SOURCESYS`),
                                    KEY `idx_bw_object_norm_sourcesys` (`BW_OBJECT_NORM`, `SOURCESYS`),
                                    KEY `idx_bw_object_norm_lookup` (`BW_OBJECT_NORM`, `BW_OBJECT_TYPE`, `SOURCESYS`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                """
            )
            conn.commit()
            return

        cur.execute(
            """
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name'
            """,
            (DB_CONFIG["database"],),
        )
        col_rows = cur.fetchall()
        col_map = {
            str(name): {
                "type": str(col_type),
                "nullable": str(is_nullable).upper() == "YES",
                "comment": str(comment or ""),
            }
            for name, col_type, is_nullable, comment in col_rows
        }

        # Migrate OBJECT_NAME -> NAME_EN when needed.
        if "NAME_EN" not in col_map:
            if "OBJECT_NAME" in col_map:
                src = col_map["OBJECT_NAME"]
                nullable_sql = "NULL" if src["nullable"] else "NOT NULL"
                escaped_comment = src["comment"].replace("'", "''")
                comment_sql = f" COMMENT '{escaped_comment}'" if src["comment"] else ""
                cur.execute(
                    f"ALTER TABLE `bw_object_name` CHANGE COLUMN `OBJECT_NAME` `NAME_EN` {src['type']} {nullable_sql}{comment_sql}"
                )
            else:
                cur.execute("ALTER TABLE `bw_object_name` ADD COLUMN `NAME_EN` VARCHAR(255) NULL")

        # Ensure NAME_DE exists.
        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name' AND COLUMN_NAME = 'NAME_DE'
            """,
            (DB_CONFIG["database"],),
        )
        if int(cur.fetchone()[0]) == 0:
            cur.execute("ALTER TABLE `bw_object_name` ADD COLUMN `NAME_DE` VARCHAR(255) NULL")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name' AND COLUMN_NAME = 'BW_OBJECT_NORM'
            """,
            (DB_CONFIG["database"],),
        )
        if int(cur.fetchone()[0]) == 0:
            cur.execute(
                "ALTER TABLE `bw_object_name` ADD COLUMN `BW_OBJECT_NORM` VARCHAR(40) NULL COMMENT 'BW object normalized to uppercase' AFTER `BW_OBJECT`"
            )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name' AND COLUMN_NAME = 'NAME_EN_NORM'
            """,
            (DB_CONFIG["database"],),
        )
        if int(cur.fetchone()[0]) == 0:
            cur.execute(
                "ALTER TABLE `bw_object_name` ADD COLUMN `NAME_EN_NORM` VARCHAR(255) NULL COMMENT 'Object Name (EN) normalized to uppercase' AFTER `NAME_EN`"
            )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name' AND COLUMN_NAME = 'NAME_DE_NORM'
            """,
            (DB_CONFIG["database"],),
        )
        if int(cur.fetchone()[0]) == 0:
            cur.execute(
                "ALTER TABLE `bw_object_name` ADD COLUMN `NAME_DE_NORM` VARCHAR(255) NULL COMMENT 'Object Name (DE) normalized to uppercase' AFTER `NAME_DE`"
            )

        # If OBJECT_NAME still exists after migration, merge then drop.
        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name' AND COLUMN_NAME = 'OBJECT_NAME'
            """,
            (DB_CONFIG["database"],),
        )
        has_object_name = int(cur.fetchone()[0]) > 0
        if has_object_name:
            cur.execute(
                """
                UPDATE `bw_object_name`
                SET `NAME_EN` = COALESCE(NULLIF(TRIM(`NAME_EN`), ''), NULLIF(TRIM(`OBJECT_NAME`), ''))
                """
            )
            cur.execute("ALTER TABLE `bw_object_name` DROP COLUMN `OBJECT_NAME`")

        cur.execute(
            """
            SELECT IS_NULLABLE, COLUMN_TYPE, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bw_object_name' AND COLUMN_NAME = 'SOURCESYS'
            """,
            (DB_CONFIG["database"],),
        )
        row = cur.fetchone()
        if not row:
            conn.commit()
            return

        is_nullable, column_type, column_comment = row
        if str(is_nullable).upper() != "YES":
            cur.execute("SHOW KEYS FROM `bw_object_name` WHERE Key_name = 'PRIMARY'")
            pk_rows = cur.fetchall()
            pk_cols = [r[4] for r in pk_rows]
            can_relax_sourcesys = True
            if "SOURCESYS" in pk_cols:
                try:
                    cur.execute("ALTER TABLE `bw_object_name` DROP PRIMARY KEY")
                except mysql.connector.Error as exc:
                    # TiDB clustered index can reject DROP PRIMARY KEY (e.g. error 8200).
                    # In that case keep existing schema and avoid blocking API startup.
                    print(f"[startup] Skip bw_object_name PK migration on current DB engine: {exc}")
                    can_relax_sourcesys = False

            if can_relax_sourcesys:
                escaped_comment = str(column_comment or "").replace("'", "''")
                comment_sql = f" COMMENT '{escaped_comment}'" if column_comment is not None else ""
                cur.execute(f"ALTER TABLE `bw_object_name` MODIFY COLUMN `SOURCESYS` {column_type} NULL{comment_sql}")

                ensure_bw_object_name_indexes()
        else:
            ensure_bw_object_name_indexes()

        cur.execute(
            """
            UPDATE `bw_object_name`
            SET
              `BW_OBJECT_NORM` = UPPER(TRIM(COALESCE(`BW_OBJECT`, ''))),
              `NAME_EN_NORM` = NULLIF(UPPER(TRIM(COALESCE(`NAME_EN`, ''))), ''),
              `NAME_DE_NORM` = NULLIF(UPPER(TRIM(COALESCE(`NAME_DE`, ''))), '')
            WHERE COALESCE(`BW_OBJECT_NORM`, '') <> UPPER(TRIM(COALESCE(`BW_OBJECT`, '')))
               OR COALESCE(`NAME_EN_NORM`, '') <> COALESCE(NULLIF(UPPER(TRIM(COALESCE(`NAME_EN`, ''))), ''), '')
               OR COALESCE(`NAME_DE_NORM`, '') <> COALESCE(NULLIF(UPPER(TRIM(COALESCE(`NAME_DE`, ''))), ''), '')
            """
        )

        cur.execute(
            """
            UPDATE `bw_object_name`
            SET `BW_OBJECT_NORM` = UPPER(TRIM(COALESCE(`BW_OBJECT`, '')))
            WHERE `BW_OBJECT_NORM` IS NULL
            """
        )

        try:
            cur.execute("ALTER TABLE `bw_object_name` MODIFY COLUMN `BW_OBJECT_NORM` VARCHAR(40) NOT NULL COMMENT 'BW object normalized to uppercase'")
        except mysql.connector.Error as exc:
            print(f"[startup] Skip bw_object_name BW_OBJECT_NORM tighten on current DB engine: {exc}")

        conn.commit()
    finally:
        cur.close()
        conn.close()


def ensure_rstran_schema() -> None:
    """Keep rstran schema aligned with SOURCE naming rule."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'rstran'
            """,
            (DB_CONFIG["database"],),
        )
        if int(cur.fetchone()[0]) == 0:
            conn.commit()
            return

        cur.execute(
            """
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'rstran'
            """,
            (DB_CONFIG["database"],),
        )
        rows = cur.fetchall()
        col_map = {str(name): (str(col_type), str(is_nullable).upper() == "YES") for name, col_type, is_nullable in rows}

        if "SOURCE" not in col_map:
            if "DATASOURCE" in col_map:
                src_type, src_nullable = col_map["DATASOURCE"]
                nullable_sql = "NULL" if src_nullable else "NOT NULL"
                cur.execute(f"ALTER TABLE `rstran` ADD COLUMN `SOURCE` {src_type} {nullable_sql}")
                cur.execute("UPDATE `rstran` SET `SOURCE` = `DATASOURCE` WHERE `SOURCE` IS NULL")
            else:
                cur.execute("ALTER TABLE `rstran` ADD COLUMN `SOURCE` VARCHAR(255) NULL")

        # Backfill SOURCE from SOURCENAME first token when still empty.
        cur.execute(
            """
            UPDATE `rstran`
            SET `SOURCE` = NULLIF(TRIM(SUBSTRING_INDEX(TRIM(`SOURCENAME`), ' ', 1)), '')
            WHERE (`SOURCE` IS NULL OR TRIM(`SOURCE`) = '')
              AND `SOURCENAME` IS NOT NULL
              AND TRIM(`SOURCENAME`) <> ''
            """
        )

        conn.commit()
    finally:
        cur.close()
        conn.close()


def ensure_auth_tables() -> None:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `users` (
              `username` VARCHAR(64) NOT NULL,
              `password_hash` VARCHAR(255) NOT NULL,
              `role` VARCHAR(16) NOT NULL DEFAULT 'user',
              `is_locked` TINYINT(1) NOT NULL DEFAULT 0,
              `failed_attempts` INT NOT NULL DEFAULT 0,
              `temp_lock_until` DATETIME NULL,
              `last_login_at` DATETIME NULL,
              `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (`username`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `user_sessions` (
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `username` VARCHAR(64) NOT NULL,
              `session_hash` CHAR(64) NOT NULL,
              `expires_at` DATETIME NOT NULL,
              `revoked` TINYINT(1) NOT NULL DEFAULT 0,
              `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              `last_seen_at` DATETIME NULL,
              PRIMARY KEY (`id`),
              UNIQUE KEY `uk_session_hash` (`session_hash`),
              KEY `idx_session_user` (`username`),
              CONSTRAINT `fk_sessions_user` FOREIGN KEY (`username`) REFERENCES `users` (`username`) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `auth_audit_logs` (
              `id` BIGINT NOT NULL AUTO_INCREMENT,
              `event_type` VARCHAR(64) NOT NULL,
              `username` VARCHAR(64) NULL,
              `actor` VARCHAR(64) NULL,
              `success` TINYINT(1) NOT NULL DEFAULT 1,
              `detail` VARCHAR(255) NULL,
              `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`id`),
              KEY `idx_auth_event` (`event_type`, `created_at`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM users")
        has_user = int(cur.fetchone()[0]) > 0
        if not has_user:
            cur.execute(
                "INSERT INTO users (username, password_hash, role, is_locked, failed_attempts) VALUES (%s, %s, 'admin', 0, 0)",
                (normalize_username(DEFAULT_ADMIN_USERNAME), hash_password(DEFAULT_ADMIN_PASSWORD)),
            )
            conn.commit()
    finally:
        cur.close()
        conn.close()


def ensure_user_hidden_object_table() -> None:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
                        CREATE TABLE IF NOT EXISTS `user_hidden_object` (
              `bw_object` VARCHAR(40) NOT NULL COMMENT 'BW Object',
                            `sourcesys` VARCHAR(25) NOT NULL DEFAULT '' COMMENT 'Source System',
                            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (`bw_object`, `sourcesys`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户隐藏对象清单';
            """
        )

        cur.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'user_del_flag'
            """,
            (DB_CONFIG["database"],),
        )
        has_legacy_table = int(cur.fetchone()[0]) > 0

        if has_legacy_table:
            cur.execute(
                """
                INSERT IGNORE INTO `user_hidden_object` (`bw_object`, `sourcesys`)
                SELECT
                    UPPER(TRIM(`bw_object`)) AS bw_object,
                    COALESCE(NULLIF(UPPER(TRIM(`sourcesys`)), ''), '') AS sourcesys
                FROM `user_del_flag`
                WHERE `bw_object` IS NOT NULL AND TRIM(`bw_object`) <> ''
                """
            )
            cur.execute("DROP TABLE IF EXISTS `user_del_flag`")

        conn.commit()
    finally:
        cur.close()
        conn.close()


def audit_log(event_type: str, username: str | None, success: bool, detail: str = "", actor: str | None = None) -> None:
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO auth_audit_logs (event_type, username, actor, success, detail)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (event_type, normalize_username(username) if username else None, normalize_username(actor) if actor else None, 1 if success else 0, detail[:255] if detail else None),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def fetch_user_by_username(username: str) -> Dict[str, object] | None:
    name = normalize_username(username)
    if not name:
        return None
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT username, password_hash, role, is_locked, failed_attempts, temp_lock_until, last_login_at
            FROM users
            WHERE username = %s
            """,
            (name,),
        )
        row = cur.fetchone()
        return row
    finally:
        cur.close()
        conn.close()


def create_session(username: str) -> tuple[str, datetime]:
    token = secrets.token_urlsafe(32)
    token_hash = hash_session_token(token)
    expires_at = utcnow() + timedelta(hours=AUTH_SESSION_HOURS)

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO user_sessions (username, session_hash, expires_at, revoked, last_seen_at)
            VALUES (%s, %s, %s, 0, %s)
            """,
            (normalize_username(username), token_hash, expires_at, utcnow()),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return token, expires_at


def revoke_session(token: str) -> None:
    token_hash = hash_session_token(token)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE user_sessions SET revoked = 1 WHERE session_hash = %s", (token_hash,))
        conn.commit()
    finally:
        cur.close()
        conn.close()


def resolve_user_from_request(request: Request) -> Dict[str, object] | None:
    raw_token = request.cookies.get(AUTH_COOKIE_NAME)
    if not raw_token:
        return None

    token_hash = hash_session_token(raw_token)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT s.username, s.expires_at, s.revoked, u.role, u.is_locked
            FROM user_sessions s
            JOIN users u ON u.username = s.username
            WHERE s.session_hash = %s
            LIMIT 1
            """,
            (token_hash,),
        )
        row = cur.fetchone()
        if not row:
            return None

        if int(row.get("revoked") or 0) == 1:
            return None
        expires_at = row.get("expires_at")
        if not isinstance(expires_at, datetime) or expires_at <= utcnow():
            cur.execute("UPDATE user_sessions SET revoked = 1 WHERE session_hash = %s", (token_hash,))
            conn.commit()
            return None
        if int(row.get("is_locked") or 0) == 1:
            return None

        cur.execute("UPDATE user_sessions SET last_seen_at = %s WHERE session_hash = %s", (utcnow(), token_hash))
        conn.commit()
        return {"username": row["username"], "role": row["role"]}
    finally:
        cur.close()
        conn.close()


def get_table_columns(table_name: str) -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (DB_CONFIG["database"], table_name),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[0] for r in rows]


def get_table_column_lengths(table_name: str) -> Dict[str, int | None]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (DB_CONFIG["database"], table_name),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    output: Dict[str, int | None] = {}
    for col_name, max_len in rows:
        output[col_name] = int(max_len) if max_len is not None else None
    return output


def get_primary_keys(table_name: str) -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [r[4] for r in rows]


def get_duplicate_check_fields(table_name: str) -> List[str]:
    custom = DUPLICATE_CHECK_FIELDS.get(table_name)
    if custom:
        return custom
    return get_primary_keys(table_name)


def count_table_rows(table_name: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    if table_name == "bw_object_name":
        cur.execute(
            """
            SELECT COUNT(*)
            FROM `bw_object_name`
            WHERE COALESCE(TRIM(`NAME_EN`), '') <> ''
               OR COALESCE(TRIM(`NAME_DE`), '') <> ''
            """
        )
    else:
        cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    value = int(cur.fetchone()[0])
    cur.close()
    conn.close()
    return value


def normalize_type_code(raw_type: str | None) -> str:
    txt = str(raw_type or "").strip().upper()
    if not txt:
        return "UNKNOWN"
    return txt


def normalize_hidden_bw_object(raw: str | None) -> str:
    return str(raw or "").strip().upper()


def normalize_hidden_sourcesys(raw: str | None) -> str:
    txt = str(raw or "").strip().upper()
    return txt if len(txt) >= 3 else ""


def row_has_transformation_logic(start_routine: object, end_routine: object, expert: object) -> bool:
    return any(str(value or "").strip() for value in (start_routine, end_routine, expert))


def extract_transformation_logic_details(start_routine: object, end_routine: object, expert: object) -> List[Dict[str, str]]:
    details: List[Dict[str, str]] = []
    for kind, value in (("start", start_routine), ("end", end_routine), ("expert", expert)):
        txt = str(value or "").strip()
        if txt:
            details.append({"kind": kind, "id": txt})
    return details


def append_or_update_edge(
    edges: List[Dict[str, object]],
    edge_by_key: Dict[Tuple[str, str, str, str], Dict[str, object]],
    source_name: str,
    target_name: str,
    source_type: str,
    target_type: str,
    tran_id: object = None,
    start_routine: object = None,
    end_routine: object = None,
    expert: object = None,
) -> None:
    edge_key = (source_name, target_name, source_type, target_type)
    logic_details = extract_transformation_logic_details(start_routine, end_routine, expert)
    tran_id_text = str(tran_id or "").strip()
    existing = edge_by_key.get(edge_key)
    if existing is None:
        edge = {
            "source": source_name,
            "target": target_name,
            "source_type": source_type,
            "target_type": target_type,
            "has_logic": bool(logic_details),
            "logic_details": logic_details,
            "logic_ids": [detail["id"] for detail in logic_details],
            "tran_ids": [tran_id_text] if tran_id_text else [],
        }
        edges.append(edge)
        edge_by_key[edge_key] = edge
        return

    if tran_id_text:
        existing_tran_ids = existing.setdefault("tran_ids", [])
        if tran_id_text not in existing_tran_ids:
            existing_tran_ids.append(tran_id_text)

    if logic_details:
        existing["has_logic"] = True
        existing_details = existing.setdefault("logic_details", [])
        existing_keys = {(str(detail.get("kind") or ""), str(detail.get("id") or "")) for detail in existing_details}
        for detail in logic_details:
            detail_key = (str(detail.get("kind") or ""), str(detail.get("id") or ""))
            if detail_key in existing_keys:
                continue
            existing_details.append(detail)
            existing_keys.add(detail_key)
        existing["logic_ids"] = [detail.get("id", "") for detail in existing_details if str(detail.get("id") or "").strip()]


def _build_graph_engine_by_source(start_name: str, max_nodes: int = 2000, max_edges: int = 5000) -> Dict[str, object]:
    """Wave expansion by SOURCENAME -> TARGETNAME with row-level deduplication."""
    seed = start_name.strip()
    if not seed:
        return {"nodes": [], "edges": [], "color_map": {}}

    conn = get_conn()
    cur = conn.cursor()

    # Row-level deduplication for stable traversal.
    visited_row_indices: Set[int] = set()
    current_keys: List[str] = [seed]
    node_types: Dict[str, str] = {seed: "UNKNOWN"}
    node_levels: Dict[str, int] = {seed: 0}
    node_object_names: Dict[str, str] = {}
    edge_seen: Set[Tuple[str, str, str, str]] = set()
    edge_by_key: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    edges: List[Dict[str, object]] = []

    try:
        cur.execute(
            """
            SELECT SOURCENAME, TARGETNAME, SOURCETYPE, TARGETTYPE, TRANID, STARTROUTINE, ENDROUTINE, EXPERT
            FROM rstran
            WHERE SOURCENAME IS NOT NULL AND TARGETNAME IS NOT NULL
            ORDER BY TRANID
            """
        )
        all_rows = cur.fetchall()

        source_to_indices: Dict[str, List[int]] = {}
        normalized_rows: List[Tuple[str, str, str, str, object, object, object, object]] = []
        for idx, row in enumerate(all_rows):
            source_name = str(row[0] or "").strip()
            target_name = str(row[1] or "").strip()
            source_type = normalize_type_code(row[2])
            target_type = normalize_type_code(row[3])
            normalized_rows.append((source_name, target_name, source_type, target_type, row[4], row[5], row[6], row[7]))
            if not source_name:
                continue
            source_to_indices.setdefault(source_name, []).append(idx)

        depth = 0
        while current_keys:
            next_keys: List[str] = []
            for key in current_keys:
                for idx in source_to_indices.get(key, []):
                    if idx in visited_row_indices:
                        continue
                    visited_row_indices.add(idx)

                    source_name, target_name, source_type, target_type, tran_id, start_routine, end_routine, expert = normalized_rows[idx]
                    if not source_name or not target_name:
                        continue

                    edge_key = (source_name, target_name, source_type, target_type)
                    if edge_key not in edge_seen:
                        edge_seen.add(edge_key)
                        append_or_update_edge(
                            edges,
                            edge_by_key,
                            source_name,
                            target_name,
                            source_type,
                            target_type,
                            tran_id,
                            start_routine,
                            end_routine,
                            expert,
                        )
                    else:
                        append_or_update_edge(
                            edges,
                            edge_by_key,
                            source_name,
                            target_name,
                            source_type,
                            target_type,
                            tran_id,
                            start_routine,
                            end_routine,
                            expert,
                        )

                    if source_name not in node_types or node_types[source_name] == "UNKNOWN":
                        node_types[source_name] = source_type
                    if target_name not in node_types or node_types[target_name] == "UNKNOWN":
                        node_types[target_name] = target_type

                    if source_name not in node_levels:
                        node_levels[source_name] = depth
                    if target_name not in node_levels:
                        node_levels[target_name] = depth + 1

                    next_keys.append(target_name)

                    if len(node_types) >= max_nodes or len(edges) >= max_edges:
                        break
                if len(node_types) >= max_nodes or len(edges) >= max_edges:
                    break

            current_keys = next_keys
            depth += 1

            if len(node_types) >= max_nodes or len(edges) >= max_edges:
                break

        # Enrich all discovered technical names with NAME_EN from bw_object_name.
        all_node_names = list(node_types.keys())
        batch_size = 500
        for start in range(0, len(all_node_names), batch_size):
            batch = [str(v).strip().upper() for v in all_node_names[start : start + batch_size] if str(v).strip()]
            if not batch:
                continue
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"""
                SELECT BW_OBJECT_NORM, MAX(NAME_EN) AS NAME_EN
                FROM bw_object_name
                WHERE BW_OBJECT_NORM IN ({placeholders})
                GROUP BY BW_OBJECT_NORM
                """,
                tuple(batch),
            )
            for bw_object, object_name in cur.fetchall():
                key = normalize_bw_object_lookup(bw_object)
                value = str(object_name or "").strip()
                if key and value:
                    node_object_names[key] = value
    finally:
        cur.close()
        conn.close()

    nodes = [
        {
            "id": name,
            "label": name,
            "object_name": node_object_names.get(name) or node_object_names.get(name.upper(), ""),
            "type": node_types.get(name, "UNKNOWN"),
            "level": int(node_levels.get(name, 0)),
            "lane": idx + 1,
        }
        for idx, name in enumerate(sorted(node_types.keys()))
    ]

    # Color is driven by node type (source/target type from rstran).
    color_map = {
        "RSDS": "#9aa0a6",
        "TRCS": "#ffd54a",
        "ADSO": "#5f8dff",
        "IOBJ": "#52c5a8",
        "HCPR": "#ff9f43",
        "ELEM": "#f36f9a",
        "DEST": "#7dd3fc",
        "UNKNOWN": "#b4bfd6",
    }

    return {
        "nodes": nodes,
        "edges": edges,
        "color_map": color_map,
        "stats": {
            "start": seed,
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def build_graph_upstream(start_name: str, max_nodes: int = 2000, max_edges: int = 5000) -> Dict[str, object]:
    """Canonical app mode: upstream (向上追溯)."""
    return _build_graph_engine_by_source(start_name, max_nodes=max_nodes, max_edges=max_edges)


def build_graph_downstream(start_name: str, max_nodes: int = 2000, max_edges: int = 5000) -> Dict[str, object]:
    """Canonical app mode: downstream (向下追溯)."""
    return _build_graph_engine_by_target(start_name, max_nodes=max_nodes, max_edges=max_edges)


def build_graph_both(start_name: str, max_nodes: int = 2000, max_edges: int = 5000) -> Dict[str, object]:
    """Canonical app mode: both (向上+向下), aligned to reference mode=3 semantics.

    Execution order follows the reference Python script:
    1) TARGETNAME -> SOURCENAME wave (mode1 path)
    2) SOURCENAME -> TARGETNAME wave (mode2 path)
    Both waves share one visited-row set so each row is emitted at most once.
    """
    seed = start_name.strip()
    if not seed:
        return {"nodes": [], "edges": [], "color_map": {}}

    conn = get_conn()
    cur = conn.cursor()

    visited_row_indices: Set[int] = set()
    node_types: Dict[str, str] = {seed: "UNKNOWN"}
    node_levels: Dict[str, int] = {seed: 0}
    node_object_names: Dict[str, str] = {}
    edge_seen: Set[Tuple[str, str, str, str]] = set()
    edge_by_key: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    edges: List[Dict[str, object]] = []

    try:
        cur.execute(
            """
            SELECT SOURCENAME, TARGETNAME, SOURCETYPE, TARGETTYPE, TRANID, STARTROUTINE, ENDROUTINE, EXPERT
            FROM rstran
            WHERE SOURCENAME IS NOT NULL AND TARGETNAME IS NOT NULL
            ORDER BY TRANID
            """
        )
        all_rows = cur.fetchall()

        source_to_indices: Dict[str, List[int]] = {}
        target_to_indices: Dict[str, List[int]] = {}
        normalized_rows: List[Tuple[str, str, str, str, object, object, object, object]] = []

        for idx, row in enumerate(all_rows):
            source_name = str(row[0] or "").strip()
            target_name = str(row[1] or "").strip()
            source_type = normalize_type_code(row[2])
            target_type = normalize_type_code(row[3])
            normalized_rows.append((source_name, target_name, source_type, target_type, row[4], row[5], row[6], row[7]))
            if source_name:
                source_to_indices.setdefault(source_name, []).append(idx)
            if target_name:
                target_to_indices.setdefault(target_name, []).append(idx)

        # Wave-1: TARGET -> SOURCE (reference mode1)
        current_keys: List[str] = [seed]
        depth = 0
        while current_keys:
            next_keys: List[str] = []
            for key in current_keys:
                for idx in target_to_indices.get(key, []):
                    if idx in visited_row_indices:
                        continue
                    visited_row_indices.add(idx)

                    source_name, target_name, source_type, target_type, tran_id, start_routine, end_routine, expert = normalized_rows[idx]
                    if not source_name or not target_name:
                        continue

                    edge_key = (source_name, target_name, source_type, target_type)
                    if edge_key not in edge_seen:
                        edge_seen.add(edge_key)
                    append_or_update_edge(
                        edges,
                        edge_by_key,
                        source_name,
                        target_name,
                        source_type,
                        target_type,
                        tran_id,
                        start_routine,
                        end_routine,
                        expert,
                    )

                    if source_name not in node_types or node_types[source_name] == "UNKNOWN":
                        node_types[source_name] = source_type
                    if target_name not in node_types or node_types[target_name] == "UNKNOWN":
                        node_types[target_name] = target_type

                    if target_name not in node_levels:
                        node_levels[target_name] = depth
                    if source_name not in node_levels:
                        node_levels[source_name] = depth + 1

                    next_keys.append(source_name)

                    if len(node_types) >= max_nodes or len(edges) >= max_edges:
                        break
                if len(node_types) >= max_nodes or len(edges) >= max_edges:
                    break

            current_keys = next_keys
            depth += 1
            if len(node_types) >= max_nodes or len(edges) >= max_edges:
                break

        # Wave-2: SOURCE -> TARGET (reference mode2) with the SAME visited rows.
        current_keys = [seed]
        depth = 0
        while current_keys:
            next_keys = []
            for key in current_keys:
                for idx in source_to_indices.get(key, []):
                    if idx in visited_row_indices:
                        continue
                    visited_row_indices.add(idx)

                    source_name, target_name, source_type, target_type, tran_id, start_routine, end_routine, expert = normalized_rows[idx]
                    if not source_name or not target_name:
                        continue

                    edge_key = (source_name, target_name, source_type, target_type)
                    if edge_key not in edge_seen:
                        edge_seen.add(edge_key)
                    append_or_update_edge(
                        edges,
                        edge_by_key,
                        source_name,
                        target_name,
                        source_type,
                        target_type,
                        tran_id,
                        start_routine,
                        end_routine,
                        expert,
                    )

                    if source_name not in node_types or node_types[source_name] == "UNKNOWN":
                        node_types[source_name] = source_type
                    if target_name not in node_types or node_types[target_name] == "UNKNOWN":
                        node_types[target_name] = target_type

                    # Use negative depth for second wave so seed stays near center.
                    if source_name not in node_levels:
                        node_levels[source_name] = -depth
                    if target_name not in node_levels:
                        node_levels[target_name] = -(depth + 1)

                    next_keys.append(target_name)

                    if len(node_types) >= max_nodes or len(edges) >= max_edges:
                        break
                if len(node_types) >= max_nodes or len(edges) >= max_edges:
                    break

            current_keys = next_keys
            depth += 1
            if len(node_types) >= max_nodes or len(edges) >= max_edges:
                break

        all_node_names = list(node_types.keys())
        batch_size = 500
        for start in range(0, len(all_node_names), batch_size):
            batch = [str(v).strip().upper() for v in all_node_names[start : start + batch_size] if str(v).strip()]
            if not batch:
                continue
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"""
                SELECT BW_OBJECT_NORM, MAX(NAME_EN) AS NAME_EN
                FROM bw_object_name
                WHERE BW_OBJECT_NORM IN ({placeholders})
                GROUP BY BW_OBJECT_NORM
                """,
                tuple(batch),
            )
            for bw_object, object_name in cur.fetchall():
                key = normalize_bw_object_lookup(bw_object)
                value = str(object_name or "").strip()
                if key and value:
                    node_object_names[key] = value
    finally:
        cur.close()
        conn.close()

    nodes = [
        {
            "id": name,
            "label": name,
            "object_name": node_object_names.get(name) or node_object_names.get(name.upper(), ""),
            "type": node_types.get(name, "UNKNOWN"),
            "level": int(node_levels.get(name, 0)),
            "lane": idx + 1,
        }
        for idx, name in enumerate(sorted(node_types.keys()))
    ]

    color_map = {
        "RSDS": "#9aa0a6",
        "TRCS": "#ffd54a",
        "ADSO": "#5f8dff",
        "IOBJ": "#52c5a8",
        "HCPR": "#ff9f43",
        "ELEM": "#f36f9a",
        "DEST": "#7dd3fc",
        "UNKNOWN": "#b4bfd6",
    }

    return {
        "nodes": nodes,
        "edges": edges,
        "color_map": color_map,
        "stats": {
            "start": seed,
            "mode": "both",
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def build_graph_full(start_name: str, max_nodes: int = 2000, max_edges: int = 5000) -> Dict[str, object]:
    """Canonical app mode: full (全量数据流), aligned to reference mode=4 semantics.

    Strategy:
    1) Treat each row as an undirected adjacency for reachability expansion.
    2) Starting from seed, expand to all connected neighbors until convergence.
    3) Render the induced directed subgraph on this connected component.

    This means mode=full keeps expanding from newly found nodes in BOTH directions,
    so direction changes are allowed at every hop.
    """
    seed = start_name.strip()
    if not seed:
        return {"nodes": [], "edges": [], "color_map": {}}

    conn = get_conn()
    cur = conn.cursor()

    node_types: Dict[str, str] = {seed: "UNKNOWN"}
    node_object_names: Dict[str, str] = {}
    edges: List[Dict[str, object]] = []

    try:
        cur.execute(
            """
            SELECT SOURCENAME, TARGETNAME, SOURCETYPE, TARGETTYPE, TRANID, STARTROUTINE, ENDROUTINE, EXPERT
            FROM rstran
            WHERE SOURCENAME IS NOT NULL AND TARGETNAME IS NOT NULL
            ORDER BY TRANID
            """
        )
        all_rows = cur.fetchall()

        normalized_rows: List[Tuple[str, str, str, str, object, object, object, object]] = []
        undirected_adj_all: Dict[str, Set[str]] = {}

        for row in all_rows:
            source_name = str(row[0] or "").strip()
            target_name = str(row[1] or "").strip()
            source_type = normalize_type_code(row[2])
            target_type = normalize_type_code(row[3])
            if not source_name or not target_name:
                continue

            normalized_rows.append((source_name, target_name, source_type, target_type, row[4], row[5], row[6], row[7]))
            undirected_adj_all.setdefault(source_name, set()).add(target_name)
            undirected_adj_all.setdefault(target_name, set()).add(source_name)

            if source_name not in node_types or node_types[source_name] == "UNKNOWN":
                node_types[source_name] = source_type
            if target_name not in node_types or node_types[target_name] == "UNKNOWN":
                node_types[target_name] = target_type

        # Full closure: expand connected component with direction switch allowed.
        all_nodes: Set[str] = set()
        queue: List[str] = [seed]
        while queue:
            cur_key = queue.pop(0)
            if cur_key in all_nodes:
                continue
            all_nodes.add(cur_key)
            for nxt in undirected_adj_all.get(cur_key, set()):
                if nxt not in all_nodes:
                    queue.append(nxt)
            if len(all_nodes) >= max_nodes:
                break

        if seed:
            all_nodes.add(seed)

        # Build induced full subgraph over the closure union.
        edge_seen: Set[Tuple[str, str, str, str]] = set()
        edge_by_key: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
        undirected_adj: Dict[str, Set[str]] = {}
        for source_name, target_name, source_type, target_type, tran_id, start_routine, end_routine, expert in normalized_rows:
            if source_name not in all_nodes or target_name not in all_nodes:
                continue
            key = (source_name, target_name, source_type, target_type)
            if key in edge_seen:
                append_or_update_edge(
                    edges,
                    edge_by_key,
                    source_name,
                    target_name,
                    source_type,
                    target_type,
                    tran_id,
                    start_routine,
                    end_routine,
                    expert,
                )
                continue
            edge_seen.add(key)
            append_or_update_edge(
                edges,
                edge_by_key,
                source_name,
                target_name,
                source_type,
                target_type,
                tran_id,
                start_routine,
                end_routine,
                expert,
            )
            undirected_adj.setdefault(source_name, set()).add(target_name)
            undirected_adj.setdefault(target_name, set()).add(source_name)
            if len(edges) >= max_edges:
                break

        # Level for full mode: shortest undirected distance from seed.
        node_levels: Dict[str, int] = {seed: 0}
        queue = [seed]
        while queue:
            cur_key = queue.pop(0)
            base = node_levels.get(cur_key, 0)
            for nxt in undirected_adj.get(cur_key, set()):
                if nxt in node_levels:
                    continue
                node_levels[nxt] = base + 1
                queue.append(nxt)

        # Enrich object names for all discovered nodes.
        all_node_names = list(all_nodes)
        batch_size = 500
        for start in range(0, len(all_node_names), batch_size):
            batch = [str(v).strip().upper() for v in all_node_names[start : start + batch_size] if str(v).strip()]
            if not batch:
                continue
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"""
                SELECT BW_OBJECT_NORM, MAX(NAME_EN) AS NAME_EN
                FROM bw_object_name
                WHERE BW_OBJECT_NORM IN ({placeholders})
                GROUP BY BW_OBJECT_NORM
                """,
                tuple(batch),
            )
            for bw_object, object_name in cur.fetchall():
                key = normalize_bw_object_lookup(bw_object)
                value = str(object_name or "").strip()
                if key and value:
                    node_object_names[key] = value
    finally:
        cur.close()
        conn.close()

    nodes = [
        {
            "id": name,
            "label": name,
            "object_name": node_object_names.get(name) or node_object_names.get(name.upper(), ""),
            "type": node_types.get(name, "UNKNOWN"),
            "level": int(node_levels.get(name, 0)),
            "lane": idx + 1,
        }
        for idx, name in enumerate(sorted(all_nodes))
    ]

    color_map = {
        "RSDS": "#9aa0a6",
        "TRCS": "#ffd54a",
        "ADSO": "#5f8dff",
        "IOBJ": "#52c5a8",
        "HCPR": "#ff9f43",
        "ELEM": "#f36f9a",
        "DEST": "#7dd3fc",
        "UNKNOWN": "#b4bfd6",
    }

    return {
        "nodes": nodes,
        "edges": edges,
        "color_map": color_map,
        "stats": {
            "start": seed,
            "mode": "full",
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def _build_graph_engine_by_target(start_name: str, max_nodes: int = 2000, max_edges: int = 5000) -> Dict[str, object]:
    """Wave expansion by TARGETNAME -> SOURCENAME with row-level deduplication."""
    seed = start_name.strip()
    if not seed:
        return {"nodes": [], "edges": [], "color_map": {}}

    conn = get_conn()
    cur = conn.cursor()

    visited_row_indices: Set[int] = set()
    current_keys: List[str] = [seed]
    node_types: Dict[str, str] = {seed: "UNKNOWN"}
    node_levels: Dict[str, int] = {seed: 0}
    node_object_names: Dict[str, str] = {}
    edge_seen: Set[Tuple[str, str, str, str]] = set()
    edge_by_key: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}
    edges: List[Dict[str, object]] = []

    try:
        cur.execute(
            """
            SELECT SOURCENAME, TARGETNAME, SOURCETYPE, TARGETTYPE, TRANID, STARTROUTINE, ENDROUTINE, EXPERT
            FROM rstran
            WHERE SOURCENAME IS NOT NULL AND TARGETNAME IS NOT NULL
            ORDER BY TRANID
            """
        )
        all_rows = cur.fetchall()

        target_to_indices: Dict[str, List[int]] = {}
        normalized_rows: List[Tuple[str, str, str, str, object, object, object, object]] = []
        for idx, row in enumerate(all_rows):
            source_name = str(row[0] or "").strip()
            target_name = str(row[1] or "").strip()
            source_type = normalize_type_code(row[2])
            target_type = normalize_type_code(row[3])
            normalized_rows.append((source_name, target_name, source_type, target_type, row[4], row[5], row[6], row[7]))
            if not target_name:
                continue
            target_to_indices.setdefault(target_name, []).append(idx)

        depth = 0
        while current_keys:
            next_keys: List[str] = []
            for key in current_keys:
                for idx in target_to_indices.get(key, []):
                    if idx in visited_row_indices:
                        continue
                    visited_row_indices.add(idx)

                    source_name, target_name, source_type, target_type, tran_id, start_routine, end_routine, expert = normalized_rows[idx]
                    if not source_name or not target_name:
                        continue

                    edge_key = (source_name, target_name, source_type, target_type)
                    if edge_key not in edge_seen:
                        edge_seen.add(edge_key)
                    append_or_update_edge(
                        edges,
                        edge_by_key,
                        source_name,
                        target_name,
                        source_type,
                        target_type,
                        tran_id,
                        start_routine,
                        end_routine,
                        expert,
                    )

                    if source_name not in node_types or node_types[source_name] == "UNKNOWN":
                        node_types[source_name] = source_type
                    if target_name not in node_types or node_types[target_name] == "UNKNOWN":
                        node_types[target_name] = target_type

                    if target_name not in node_levels:
                        node_levels[target_name] = depth
                    if source_name not in node_levels:
                        node_levels[source_name] = depth + 1

                    # Continue matching next key by SOURCENAME.
                    next_keys.append(source_name)

                    if len(node_types) >= max_nodes or len(edges) >= max_edges:
                        break
                if len(node_types) >= max_nodes or len(edges) >= max_edges:
                    break

            current_keys = next_keys
            depth += 1

            if len(node_types) >= max_nodes or len(edges) >= max_edges:
                break

        all_node_names = list(node_types.keys())
        batch_size = 500
        for start in range(0, len(all_node_names), batch_size):
            batch = [str(v).strip().upper() for v in all_node_names[start : start + batch_size] if str(v).strip()]
            if not batch:
                continue
            placeholders = ",".join(["%s"] * len(batch))
            cur.execute(
                f"""
                SELECT BW_OBJECT_NORM, MAX(NAME_EN) AS NAME_EN
                FROM bw_object_name
                WHERE BW_OBJECT_NORM IN ({placeholders})
                GROUP BY BW_OBJECT_NORM
                """,
                tuple(batch),
            )
            for bw_object, object_name in cur.fetchall():
                key = normalize_bw_object_lookup(bw_object)
                value = str(object_name or "").strip()
                if key and value:
                    node_object_names[key] = value
    finally:
        cur.close()
        conn.close()

    nodes = [
        {
            "id": name,
            "label": name,
            "object_name": node_object_names.get(name) or node_object_names.get(name.upper(), ""),
            "type": node_types.get(name, "UNKNOWN"),
            "level": int(node_levels.get(name, 0)),
            "lane": idx + 1,
        }
        for idx, name in enumerate(sorted(node_types.keys()))
    ]

    color_map = {
        "RSDS": "#9aa0a6",
        "TRCS": "#ffd54a",
        "ADSO": "#5f8dff",
        "IOBJ": "#52c5a8",
        "HCPR": "#ff9f43",
        "ELEM": "#f36f9a",
        "DEST": "#7dd3fc",
        "UNKNOWN": "#b4bfd6",
    }

    return {
        "nodes": nodes,
        "edges": edges,
        "color_map": color_map,
        "stats": {
            "start": seed,
            "node_count": len(nodes),
            "edge_count": len(edges),
        },
    }


def upsert_status(table_name: str, item_count: int = 0) -> Dict[str, str | int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO import_status (table_name, last_import_at, last_import_count)
        VALUES (%s, NOW(), %s)
        ON DUPLICATE KEY UPDATE
          last_import_at = VALUES(last_import_at),
          last_import_count = VALUES(last_import_count)
        """,
        (table_name, item_count),
    )
    conn.commit()
    cur.execute(
        """
        SELECT DATE_FORMAT(last_import_at, '%Y-%m-%d %H:%i'), last_import_count
        FROM import_status
        WHERE table_name = %s
        """,
        (table_name,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return {
        "last_update": row[0] if row and row[0] else "--",
        "last_count": int(row[1]) if row and row[1] is not None else 0,
    }


def parse_upload_to_dataframe(
    upload_file: UploadFile,
    sheet_name: str | None,
    header_row_num: int = 1,
) -> pd.DataFrame:
    filename = (upload_file.filename or "").lower()
    content = upload_file.file.read()
    header_index = max(0, int(header_row_num or 1) - 1)

    if filename.endswith(".csv"):
        return pd.read_csv(io.BytesIO(content), dtype=str, header=header_index).fillna("")

    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        target_sheet = sheet_name if sheet_name else 0
        return pd.read_excel(
            io.BytesIO(content),
            sheet_name=target_sheet,
            dtype=str,
            header=header_index,
        ).fillna("")

    raise HTTPException(status_code=400, detail="Unsupported file type, only xlsx/xls/csv")


def apply_rstran_logic(mapped_df: pd.DataFrame) -> pd.DataFrame:
    if "SOURCENAME" in mapped_df.columns:
        split_vals = mapped_df["SOURCENAME"].astype(str).str.strip().str.split(r"\s+", n=1, expand=True)
        first_part = split_vals[0].fillna("") if 0 in split_vals.columns else ""
        second_part = split_vals[1].fillna("") if 1 in split_vals.columns else ""

        if "SOURCE" in mapped_df.columns:
            mapped_df["SOURCE"] = first_part
        if "DATASOURCE" in mapped_df.columns:
            # Compatibility with legacy schema/exports.
            mapped_df["DATASOURCE"] = first_part
        if "SOURCESYS" in mapped_df.columns:
            mapped_df["SOURCESYS"] = second_part

    if "SOURCESYS" in mapped_df.columns:
        # Import rule: blank or too-short SOURCESYS should be treated as empty.
        src = mapped_df["SOURCESYS"].fillna("").astype(str).str.strip()
        mapped_df["SOURCESYS"] = src.where(src.str.len() >= 3, "")

    return mapped_df


def apply_bw_object_name_logic(mapped_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize object-name records."""

    for col in ("BW_OBJECT", "BW_OBJECT_TYPE", "NAME_EN", "NAME_DE", "SOURCESYS"):
        if col in mapped_df.columns:
            mapped_df[col] = mapped_df[col].astype(str).str.strip()

    if "SOURCESYS" in mapped_df.columns:
        # Keep SOURCESYS quality consistent with rstran import rule.
        src = mapped_df["SOURCESYS"].fillna("").astype(str).str.strip()
        mapped_df["SOURCESYS"] = src.where(src.str.len() >= 3, "")

    if "BW_OBJECT" in mapped_df.columns and "BW_OBJECT_NORM" in mapped_df.columns:
        mapped_df["BW_OBJECT_NORM"] = mapped_df["BW_OBJECT"].fillna("").astype(str).str.strip().str.upper()

    if "NAME_EN_NORM" in mapped_df.columns:
        if "NAME_EN" in mapped_df.columns:
            mapped_df["NAME_EN_NORM"] = mapped_df["NAME_EN"].fillna("").astype(str).str.strip().str.upper()
        else:
            mapped_df["NAME_EN_NORM"] = ""
        mapped_df.loc[mapped_df["NAME_EN_NORM"] == "", "NAME_EN_NORM"] = None

    if "NAME_DE_NORM" in mapped_df.columns:
        if "NAME_DE" in mapped_df.columns:
            mapped_df["NAME_DE_NORM"] = mapped_df["NAME_DE"].fillna("").astype(str).str.strip().str.upper()
        else:
            mapped_df["NAME_DE_NORM"] = ""
        mapped_df.loc[mapped_df["NAME_DE_NORM"] == "", "NAME_DE_NORM"] = None

    return mapped_df


def check_duplicates_by_mapped_columns(
    source_df: pd.DataFrame,
    mapped_df: pd.DataFrame,
    mapping: Dict[str, str],
    table_name: str,
    key_fields: List[str],
) -> None:
    """Validate duplicates by DB primary keys using dynamically mapped Excel columns."""
    if not key_fields:
        return

    key_df = pd.DataFrame(index=mapped_df.index)
    mapped_sources: Dict[str, str] = {}

    for db_field in key_fields:
        source_field = mapping.get(db_field, "")
        if source_field in source_df.columns:
            series = source_df[source_field].astype(str).str.strip()
            mapped_sources[db_field] = source_field
        elif db_field in mapped_df.columns:
            # Fallback for logic-generated columns.
            series = mapped_df[db_field].astype(str).str.strip()
            mapped_sources[db_field] = f"{db_field}(logic)"
        else:
            series = pd.Series(["" for _ in range(len(mapped_df))], index=mapped_df.index)
            mapped_sources[db_field] = "<未映射>"

        key_df[db_field] = series.map(normalize_cell)

    # Rows with missing key parts are left for DB NOT NULL checks.
    valid_keys = key_df.dropna(subset=key_fields)
    if valid_keys.empty:
        return

    dup_mask = valid_keys.duplicated(subset=key_fields, keep=False)
    if not dup_mask.any():
        return

    dup_rows = valid_keys[dup_mask]
    dup_keys = list(dict.fromkeys(tuple(str(v) for v in row) for row in dup_rows.to_numpy().tolist()))
    preview = "; ".join("+".join(key) for key in dup_keys[:5])
    mapping_preview = ", ".join(f"{k}->{v}" for k, v in mapped_sources.items())
    raise HTTPException(
        status_code=400,
        detail=(
            f"导入失败：检测到重复组合键({'+'.join(key_fields)})，共{len(dup_keys)}组。"
            f"映射列：{mapping_preview}。示例：{preview}。"
        ),
    )


def collapse_duplicate_rows_by_keys(
    mapped_df: pd.DataFrame,
    table_name: str,
    key_fields: List[str],
) -> tuple[pd.DataFrame, int]:
    """Collapse duplicate key rows in source data, keeping the last row for each key."""
    if not key_fields:
        return mapped_df, 0

    key_df = pd.DataFrame(index=mapped_df.index)

    for db_field in key_fields:
        if db_field in mapped_df.columns:
            series = mapped_df[db_field].astype(str).str.strip()
        else:
            series = pd.Series(["" for _ in range(len(mapped_df))], index=mapped_df.index)

        key_df[db_field] = series.map(normalize_cell)

    valid_keys = key_df.dropna(subset=key_fields)
    if valid_keys.empty:
        return mapped_df, 0

    keep_valid = ~valid_keys.duplicated(subset=key_fields, keep="last")
    keep_indices = set(valid_keys.index[keep_valid])

    keep_mask = pd.Series(True, index=mapped_df.index)
    keep_mask.loc[valid_keys.index] = valid_keys.index.isin(keep_indices)

    dropped_count = int((~keep_mask).sum())
    if dropped_count <= 0:
        return mapped_df, 0

    return mapped_df.loc[keep_mask].reset_index(drop=True), dropped_count


def check_missing_primary_keys(mapped_df: pd.DataFrame, table_name: str, key_fields: List[str]) -> None:
    """Fail early when primary-key fields become empty after preprocessing/normalization."""
    if not key_fields:
        return

    existing_keys = [k for k in key_fields if k in mapped_df.columns]
    if not existing_keys:
        return

    nullable_fields = NULLABLE_KEY_FIELDS.get(table_name, set())
    required_keys = [k for k in existing_keys if k not in nullable_fields]
    if not required_keys:
        return

    missing_mask = mapped_df[required_keys].isna().any(axis=1)
    missing_count = int(missing_mask.sum())
    if missing_count == 0:
        return

    missing_field_counts = {k: int(mapped_df[k].isna().sum()) for k in required_keys}
    summary = ", ".join(f"{k}:{v}行为空" for k, v in missing_field_counts.items() if v > 0)
    detail = (
        f"导入失败：主键字段存在空值，无法写入 {table_name}。"
        f"共{missing_count}行主键不完整；{summary}。"
    )

    raise HTTPException(status_code=400, detail=detail)


def normalize_cell(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return None
        if txt.lower() in {"nan", "none", "null"}:
            return None
        return txt
    return value


def clamp_to_length(value, max_len: int | None):
    if value is None or max_len is None:
        return value
    text = str(value)
    return text[:max_len] if len(text) > max_len else text


PUBLIC_API_PATHS = {
    "/api/auth/login",
}


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/"):
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

    if path in PUBLIC_API_PATHS:
        return await call_next(request)

    user = resolve_user_from_request(request)
    if not user:
        return JSONResponse(status_code=401, content={"detail": "未登录或会话已过期"})

    request.state.current_user = user

    if path.startswith("/api/admin/") and user.get("role") != "admin":
        return JSONResponse(status_code=403, content={"detail": "需要管理员权限"})

    return await call_next(request)


@app.on_event("startup")
def startup() -> None:
    ensure_status_table()
    ensure_rstran_schema()
    ensure_bw_object_name_schema()
    ensure_auth_tables()
    ensure_user_hidden_object_table()


@app.post("/api/auth/login")
def auth_login(payload: LoginRequest, request: Request, response: Response) -> Dict[str, object]:
    username = normalize_username(payload.username)
    password = str(payload.password or "")
    if not username or not password:
        raise HTTPException(status_code=400, detail="请输入用户名和密码")

    user = fetch_user_by_username(username)
    if not user:
        audit_log("login", username, False, detail="user_not_found")
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    temp_lock_until = user.get("temp_lock_until")
    if temp_lock_until and isinstance(temp_lock_until, datetime) and temp_lock_until > utcnow():
        remain_seconds = max(1, int((temp_lock_until - utcnow()).total_seconds()))
        remain_minutes = max(1, (remain_seconds + 59) // 60)
        audit_log("login", username, False, detail="temp_locked")
        raise HTTPException(status_code=423, detail=f"登录失败次数过多，请 {remain_minutes} 分钟后再试")
    if int(user.get("is_locked") or 0) == 1:
        audit_log("login", username, False, detail="admin_locked")
        raise HTTPException(status_code=423, detail="用户已被锁定，请联系管理员")

    if not verify_password(password, str(user.get("password_hash") or "")):
        failed_attempts = int(user.get("failed_attempts") or 0) + 1
        lock_until = None
        lock_triggered = False
        if failed_attempts >= AUTH_LOGIN_MAX_FAILS:
            lock_until = utcnow() + timedelta(minutes=AUTH_TEMP_LOCK_MINUTES)
            failed_attempts = 0
            lock_triggered = True

        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "UPDATE users SET failed_attempts = %s, temp_lock_until = %s WHERE username = %s",
                (failed_attempts, lock_until, username),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        if lock_triggered:
            audit_log("login", username, False, detail="lock_triggered")
            raise HTTPException(status_code=423, detail=f"登录失败次数过多，请 {AUTH_TEMP_LOCK_MINUTES} 分钟后再试")

        remain_attempts = max(0, AUTH_LOGIN_MAX_FAILS - failed_attempts)
        audit_log("login", username, False, detail="invalid_password")
        raise HTTPException(status_code=401, detail=f"用户名或密码错误，还可尝试 {remain_attempts} 次")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET failed_attempts = 0, temp_lock_until = NULL, last_login_at = %s WHERE username = %s",
            (utcnow(), username),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    token, expires_at = create_session(username)
    max_age = int(max(1, (expires_at - utcnow()).total_seconds()))
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=AUTH_COOKIE_SECURE or AUTH_COOKIE_SAMESITE == "none",
        samesite=AUTH_COOKIE_SAMESITE,
        domain=AUTH_COOKIE_DOMAIN,
        max_age=max_age,
        path="/",
    )
    audit_log("login", username, True)
    return {"username": username, "role": user.get("role", "user")}


@app.post("/api/auth/logout")
def auth_logout(request: Request, response: Response) -> Dict[str, str]:
    raw_token = request.cookies.get(AUTH_COOKIE_NAME)
    actor = None
    if hasattr(request.state, "current_user") and request.state.current_user:
        actor = request.state.current_user.get("username")
    if raw_token:
        revoke_session(raw_token)
    response.delete_cookie(AUTH_COOKIE_NAME, path="/", domain=AUTH_COOKIE_DOMAIN)
    audit_log("logout", actor, True)
    return {"message": "ok"}


@app.get("/api/auth/me")
def auth_me(request: Request) -> Dict[str, str]:
    user = getattr(request.state, "current_user", None)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    return {"username": user.get("username", ""), "role": user.get("role", "user")}


@app.post("/api/auth/change-password")
def auth_change_password(payload: ChangePasswordRequest, request: Request) -> Dict[str, str]:
    user = getattr(request.state, "current_user", None)
    if not user:
        raise HTTPException(status_code=401, detail="未登录")

    username = normalize_username(user.get("username", ""))
    row = fetch_user_by_username(username)
    if not row:
        raise HTTPException(status_code=404, detail="用户不存在")
    if not verify_password(payload.current_password, str(row.get("password_hash") or "")):
        audit_log("change_password", username, False, detail="wrong_current_password", actor=username)
        raise HTTPException(status_code=400, detail="当前密码错误")

    validate_password_strength(payload.new_password)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET password_hash = %s, failed_attempts = 0, temp_lock_until = NULL WHERE username = %s",
            (hash_password(payload.new_password), username),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    audit_log("change_password", username, True, actor=username)
    return {"message": "密码修改成功"}


@app.get("/api/admin/users")
def admin_list_users() -> Dict[str, List[Dict[str, object]]]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT username, role, is_locked, temp_lock_until, last_login_at, created_at
            FROM users
            ORDER BY created_at ASC, username ASC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    users = []
    for row in rows:
        users.append(
            {
                "username": row.get("username", ""),
                "role": row.get("role", "user"),
                "is_locked": bool(row.get("is_locked") or 0),
                "temp_lock_until": row.get("temp_lock_until").isoformat() if row.get("temp_lock_until") else None,
                "last_login_at": row.get("last_login_at").isoformat() if row.get("last_login_at") else None,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            }
        )
    return {"users": users}


@app.post("/api/admin/users")
def admin_create_user(payload: AdminCreateUserRequest, request: Request) -> Dict[str, str]:
    actor = request.state.current_user.get("username") if hasattr(request.state, "current_user") else None
    username = normalize_username(payload.username)
    role = str(payload.role or "").strip().lower()
    if not username:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    if role not in {"admin", "user"}:
        raise HTTPException(status_code=400, detail="角色必须是 admin 或 user")
    validate_password_strength(payload.password)

    if fetch_user_by_username(username):
        raise HTTPException(status_code=409, detail="用户名已存在")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (username, password_hash, role, is_locked, failed_attempts) VALUES (%s, %s, %s, 0, 0)",
            (username, hash_password(payload.password), role),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    audit_log("admin_create_user", username, True, actor=actor)
    return {"message": "用户创建成功"}


@app.post("/api/admin/users/{username}/lock")
def admin_lock_user(username: str, request: Request) -> Dict[str, str]:
    actor = request.state.current_user.get("username") if hasattr(request.state, "current_user") else None
    target = normalize_username(username)
    actor_name = normalize_username(actor or "")
    if target == actor_name:
        raise HTTPException(status_code=400, detail="不能锁定当前登录用户")
    if not fetch_user_by_username(target):
        raise HTTPException(status_code=404, detail="用户不存在")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE users SET is_locked = 1 WHERE username = %s", (target,))
        cur.execute("UPDATE user_sessions SET revoked = 1 WHERE username = %s", (target,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    audit_log("admin_lock_user", target, True, actor=actor)
    return {"message": "用户已锁定"}


@app.post("/api/admin/users/{username}/unlock")
def admin_unlock_user(username: str, request: Request) -> Dict[str, str]:
    actor = request.state.current_user.get("username") if hasattr(request.state, "current_user") else None
    target = normalize_username(username)
    if not fetch_user_by_username(target):
        raise HTTPException(status_code=404, detail="用户不存在")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE users SET is_locked = 0, failed_attempts = 0, temp_lock_until = NULL WHERE username = %s",
            (target,),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    audit_log("admin_unlock_user", target, True, actor=actor)
    return {"message": "用户已解锁"}


@app.post("/api/admin/users/{username}/reset-password")
def admin_reset_password(username: str, payload: AdminResetPasswordRequest, request: Request) -> Dict[str, str]:
    actor = request.state.current_user.get("username") if hasattr(request.state, "current_user") else None
    target = normalize_username(username)
    if not fetch_user_by_username(target):
        raise HTTPException(status_code=404, detail="用户不存在")

    validate_password_strength(payload.new_password)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE users
            SET password_hash = %s, failed_attempts = 0, temp_lock_until = NULL
            WHERE username = %s
            """,
            (hash_password(payload.new_password), target),
        )
        cur.execute("UPDATE user_sessions SET revoked = 1 WHERE username = %s", (target,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    audit_log("admin_reset_password", target, True, actor=actor)
    return {"message": "密码重置成功"}


@app.delete("/api/admin/users/{username}")
def admin_delete_user(username: str, request: Request) -> Dict[str, str]:
    actor = request.state.current_user.get("username") if hasattr(request.state, "current_user") else None
    target = normalize_username(username)
    actor_name = normalize_username(actor or "")
    if not target:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    if target == actor_name:
        raise HTTPException(status_code=400, detail="不能删除当前登录用户")

    row = fetch_user_by_username(target)
    if not row:
        raise HTTPException(status_code=404, detail="用户不存在")

    conn = get_conn()
    cur = conn.cursor()
    try:
        if str(row.get("role") or "user").lower() == "admin":
            cur.execute("SELECT COUNT(*) FROM users WHERE role = 'admin'")
            admin_count = int(cur.fetchone()[0] or 0)
            if admin_count <= 1:
                raise HTTPException(status_code=400, detail="至少保留一个管理员用户")

        cur.execute("DELETE FROM users WHERE username = %s", (target,))
        conn.commit()
    finally:
        cur.close()
        conn.close()

    audit_log("admin_delete_user", target, True, actor=actor)
    return {"message": "用户删除成功"}


@app.get("/api/import-status")
def get_import_status() -> Dict[str, Dict[str, str | int]]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT table_name, last_import_at, last_import_count FROM import_status")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    response: Dict[str, Dict[str, str]] = {}
    for row in rows:
        dt = row["last_import_at"]
        response[row["table_name"]] = {
            "last_update": dt.strftime("%Y-%m-%d %H:%M") if isinstance(dt, datetime) else "--",
            "last_count": int(row["last_import_count"] or 0),
        }

    for table in ALLOWED_TABLES:
        response.setdefault(table, {"last_update": "--", "last_count": 0})

    # Card count rule: bw_object_name should reflect rows with a non-empty NAME_EN/NAME_DE.
    response["bw_object_name"]["last_count"] = count_table_rows("bw_object_name")

    return response


@app.post("/api/import-status/upsert")
def upsert_import_status(payload: ImportStatusUpdate) -> Dict[str, str | int]:
    table_name = payload.table_name.strip()
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Unsupported table_name")

    status = upsert_status(table_name, count_table_rows(table_name))
    return {"table_name": table_name, "last_update": status["last_update"], "last_count": status["last_count"]}


def _build_search_like_pattern(keyword: str) -> str:
    kw = str(keyword or "").strip()
    escaped = kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    wildcard = escaped.replace("*", "%")
    if "%" in wildcard:
        return wildcard
    return f"%{wildcard}%"


def fetch_searchable_bw_objects(keyword: str, limit: int) -> tuple[str, list[dict[str, str]]]:
    kw = normalize_bw_object_lookup(keyword)

    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    try:
        if len(kw) >= 3:
            like_kw = _build_search_like_pattern(kw)
            cur.execute(
                f"""
                SELECT DISTINCT
                    TRIM(BW_OBJECT) AS id,
                    COALESCE(TRIM(SOURCESYS), '') AS source,
                    COALESCE(TRIM(BW_OBJECT_TYPE), '') AS type,
                    COALESCE(NULLIF(TRIM(NAME_EN), ''), NULLIF(TRIM(NAME_DE), ''), '') AS object_desc
                FROM bw_object_name
                WHERE (
                    BW_OBJECT_NORM LIKE %s ESCAPE '\\\\'
                    OR COALESCE(NAME_EN_NORM, '') LIKE %s ESCAPE '\\\\'
                    OR COALESCE(NAME_DE_NORM, '') LIKE %s ESCAPE '\\\\'
                )
                  AND BW_OBJECT IS NOT NULL
                  AND TRIM(BW_OBJECT) <> ''
                ORDER BY id, source, type
                LIMIT {int(limit)}
                """,
                (like_kw, like_kw, like_kw),
            )
            mode = "search"
        else:
            cur.execute(
                f"""
                SELECT DISTINCT
                    TRIM(BW_OBJECT) AS id,
                    COALESCE(TRIM(SOURCESYS), '') AS source,
                    COALESCE(TRIM(BW_OBJECT_TYPE), '') AS type,
                    COALESCE(NULLIF(TRIM(NAME_EN), ''), NULLIF(TRIM(NAME_DE), ''), '') AS object_desc
                FROM bw_object_name
                WHERE BW_OBJECT IS NOT NULL AND TRIM(BW_OBJECT) <> ''
                ORDER BY id, source, type
                LIMIT {int(limit)}
                """
            )
            mode = "default"

        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    items = []
    for row in rows:
        bw_object = str(row.get("id") or "").strip()
        source_sys = str(row.get("source") or "").strip()
        bw_type = str(row.get("type") or "").strip()
        object_name = str(row.get("object_desc") or "").strip()
        items.append(
            {
                "type": bw_type,
                "id": bw_object,
                "source": source_sys,
                "desc": object_name,
            }
        )

    return mode, items


@app.get("/api/search-more/bw-object-name")
def search_more_bw_object_name(keyword: str = Query(default="")) -> Dict[str, object]:
    kw = (keyword or "").strip()
    mode, items = fetch_searchable_bw_objects(kw, 100)

    return {
        "mode": mode,
        "keyword": kw,
        "count": len(items),
        "items": items,
    }


@app.get("/api/search/bw-object-name")
def search_bw_object_name(keyword: str = Query(default="")) -> Dict[str, object]:
    kw = (keyword or "").strip()
    if len(kw) < 3:
        return {"mode": "history", "keyword": kw, "count": 0, "items": []}
    _, items = fetch_searchable_bw_objects(kw, 5)

    return {
        "mode": "search",
        "keyword": kw,
        "count": len(items),
        "items": items,
    }


@app.get("/api/flow/trace")
def flow_trace(
    start_name: str = Query(...),
    mode: str = Query("downstream"),
    start_source: str = Query(default=""),
    start_type: str = Query(default=""),
) -> Dict[str, object]:
    requested_mode = (mode or "").strip().lower()
    normalized_start_name = normalize_bw_object_lookup(start_name)
    normalized_source = normalize_bw_object_lookup(start_source)
    normalized_type = normalize_type_code(start_type)

    resolved_start_name = normalized_start_name
    candidate_names: List[str] = [normalized_start_name]

    # Datasource start node needs special resolution.
    # rstran stores relationship traversal in SOURCENAME/TARGETNAME, but datasource identity
    # should be anchored by SOURCE + SOURCESYS to avoid ambiguous/space-padded names.
    conn = get_conn()
    cur = conn.cursor()
    try:
        if normalized_type == "RSDS" and normalized_source and normalized_start_name:
            source_col = "SOURCE" if "SOURCE" in get_table_columns("rstran") else "DATASOURCE"
            cur.execute(
                f"""
                SELECT SOURCENAME
                FROM rstran
                WHERE UPPER(TRIM({source_col})) = UPPER(%s)
                  AND UPPER(TRIM(SOURCESYS)) = UPPER(%s)
                  AND SOURCENAME IS NOT NULL
                  AND TRIM(SOURCENAME) <> ''
                ORDER BY TRANID
                LIMIT 1
                """,
                (normalized_start_name, normalized_source),
            )
            row = cur.fetchone()
            if row and row[0]:
                resolved_start_name = str(row[0]).strip()

        if resolved_start_name and resolved_start_name != normalized_start_name:
            candidate_names.insert(0, resolved_start_name)

        if normalized_source:
            # Fallback: datasource often appears in rstran as "<TECH_NAME> <SOURCESYS>".
            candidate_names.insert(0, f"{normalized_start_name} {normalized_source}".strip())

        for candidate in candidate_names:
            if not candidate:
                continue
            cur.execute(
                """
                SELECT 1
                FROM rstran
                WHERE UPPER(TRIM(SOURCENAME)) = UPPER(%s)
                   OR UPPER(TRIM(TARGETNAME)) = UPPER(%s)
                LIMIT 1
                """,
                (candidate, candidate),
            )
            if cur.fetchone():
                resolved_start_name = candidate
                if normalized_type in {"SOURCE", "DATASOURCE", "RSDS"} and normalized_source:
                    break
    finally:
        cur.close()
        conn.close()

    if requested_mode == "downstream":
        graph = build_graph_downstream(resolved_start_name)
    elif requested_mode == "upstream":
        graph = build_graph_upstream(resolved_start_name)
    elif requested_mode == "both":
        graph = build_graph_both(resolved_start_name)
    elif requested_mode == "full":
        graph = build_graph_full(resolved_start_name)
    else:
        raise HTTPException(
            status_code=400,
            detail=(
                "当前仅支持模式: "
                "upstream(向上追溯：从当前对象向数据上游展开), "
                "downstream(向下追溯：从当前对象向数据源方向展开), "
                "both(向上+向下：数据流1+数据流2), "
                "full(全量数据流：向上+向下+所有节点关联的数据流)"
            ),
        )

    return {
        "mode": requested_mode,
        "start_name": normalized_start_name,
        "start_source": normalized_source,
        "start_type": normalized_type,
        "resolved_start_name": resolved_start_name,
        "graph": graph,
    }


@app.get("/api/flow/hidden-objects")
def list_hidden_objects() -> Dict[str, object]:
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT bw_object, sourcesys
            FROM user_hidden_object
            ORDER BY bw_object, sourcesys
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    items = [
        {
            "bw_object": str(r.get("bw_object") or "").strip(),
            "sourcesys": str(r.get("sourcesys") or "").strip(),
        }
        for r in rows
    ]
    return {"count": len(items), "items": items}


@app.post("/api/flow/hidden-objects")
def add_hidden_object(payload: HiddenObjectRequest) -> Dict[str, object]:
    bw_object = normalize_hidden_bw_object(payload.bw_object)
    sourcesys = normalize_hidden_sourcesys(payload.sourcesys)
    if not bw_object:
        raise HTTPException(status_code=400, detail="bw_object 不能为空")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO user_hidden_object (bw_object, sourcesys)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE created_at = created_at
            """,
            (bw_object, sourcesys),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {"ok": True, "bw_object": bw_object, "sourcesys": sourcesys}


@app.delete("/api/flow/hidden-objects")
def remove_hidden_object(
    bw_object: str = Query(...),
    sourcesys: str = Query(default=""),
) -> Dict[str, object]:
    normalized_bw_object = normalize_hidden_bw_object(bw_object)
    normalized_sourcesys = normalize_hidden_sourcesys(sourcesys)
    if not normalized_bw_object:
        raise HTTPException(status_code=400, detail="bw_object 不能为空")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM user_hidden_object WHERE bw_object = %s AND sourcesys = %s",
            (normalized_bw_object, normalized_sourcesys),
        )
        deleted = int(cur.rowcount or 0)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {
        "ok": True,
        "deleted": deleted,
        "bw_object": normalized_bw_object,
        "sourcesys": normalized_sourcesys,
    }


def sync_bw_object_name_from_rstran(cur) -> Dict[str, int]:
    """Upsert bw_object_name from rstran in two passes.

    Pass-1:
      BW_OBJECT <- rstran.SOURCE
      SOURCESYS <- rstran.SOURCESYS
      BW_OBJECT_TYPE <- rstran.SOURCETYPE

    Pass-2:
      BW_OBJECT <- rstran.TARGETNAME
      SOURCESYS <- NULL
      BW_OBJECT_TYPE <- rstran.TARGETTYPE
    """
    rstran_cols = set(get_table_columns("rstran"))
    source_col = "SOURCE" if "SOURCE" in rstran_cols else "DATASOURCE"

    pass1_subquery = f"""
        SELECT DISTINCT
            NULLIF(TRIM(`{source_col}`), '') AS BW_OBJECT,
            COALESCE(NULLIF(TRIM(`SOURCESYS`), ''), '') AS SOURCESYS,
            NULLIF(TRIM(`SOURCETYPE`), '') AS BW_OBJECT_TYPE
        FROM `rstran`
        WHERE `{source_col}` IS NOT NULL AND TRIM(`{source_col}`) <> ''
          AND `SOURCETYPE` IS NOT NULL AND TRIM(`SOURCETYPE`) <> ''
    """

    pass2_subquery = """
        SELECT DISTINCT
            NULLIF(TRIM(`TARGETNAME`), '') AS BW_OBJECT,
            '' AS SOURCESYS,
            NULLIF(TRIM(`TARGETTYPE`), '') AS BW_OBJECT_TYPE
        FROM `rstran`
        WHERE `TARGETNAME` IS NOT NULL AND TRIM(`TARGETNAME`) <> ''
          AND `TARGETTYPE` IS NOT NULL AND TRIM(`TARGETTYPE`) <> ''
    """

    def run_pass(subquery: str) -> tuple[int, int]:
        cur.execute(
            f"""
            UPDATE `bw_object_name` b
            JOIN ({subquery}) s
              ON b.`BW_OBJECT` <=> s.`BW_OBJECT`
             AND b.`SOURCESYS` <=> s.`SOURCESYS`
            SET b.`BW_OBJECT_TYPE` = COALESCE(NULLIF(TRIM(b.`BW_OBJECT_TYPE`), ''), s.`BW_OBJECT_TYPE`)
            """
        )
        updated = int(cur.rowcount or 0)

        cur.execute(
            f"""
            INSERT INTO `bw_object_name` (`BW_OBJECT`, `SOURCESYS`, `BW_OBJECT_TYPE`, `NAME_EN`, `NAME_DE`)
            SELECT s.`BW_OBJECT`, s.`SOURCESYS`, s.`BW_OBJECT_TYPE`, NULL, NULL
            FROM ({subquery}) s
            WHERE NOT EXISTS (
                SELECT 1
                FROM `bw_object_name` b
                WHERE b.`BW_OBJECT` <=> s.`BW_OBJECT`
                  AND b.`SOURCESYS` <=> s.`SOURCESYS`
            )
            """
        )
        inserted = int(cur.rowcount or 0)
        return inserted, updated

    p1_inserted, p1_updated = run_pass(pass1_subquery)
    p2_inserted, p2_updated = run_pass(pass2_subquery)

    return {
        "inserted": int(p1_inserted + p2_inserted),
        "updated": int(p1_updated + p2_updated),
    }


@app.post("/api/import/execute")
def execute_import(
    table_name: str = Form(...),
    mapping_json: str = Form(...),
    sheet_name: str = Form(""),
    header_row_num: int = Form(1),
    duplicate_mode: str = Form("fail"),
    file: UploadFile = File(...),
) -> Dict[str, str | int]:
    duplicate_mode = str(duplicate_mode or "fail").strip().lower()
    if duplicate_mode not in {"fail", "continue", "update"}:
        raise HTTPException(status_code=400, detail="duplicate_mode must be fail or continue or update")

    table_name = table_name.strip()
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Unsupported table_name")

    try:
        mapping = json.loads(mapping_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid mapping_json") from exc

    if not isinstance(mapping, dict):
        raise HTTPException(status_code=400, detail="mapping_json must be an object")

    header_row_num = max(1, min(int(header_row_num or 1), 10))

    try:
        source_df = parse_upload_to_dataframe(file, sheet_name or None, header_row_num)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"导入失败：标题行数第{header_row_num}行超出文件有效范围，请调整后重试。",
        ) from exc

    if source_df.empty:
        filename = str(file.filename or "").strip() or "<unknown>"
        target_sheet = str(sheet_name or "").strip() or "首个Sheet/CSV"
        raise HTTPException(
            status_code=400,
            detail=(
                f"导入失败：文件 {filename} 在 {target_sheet}（标题行=第{header_row_num}行）未读取到可导入数据行。"
                "请确认文件包含表头且至少有 1 行数据，并检查是否选对了 Sheet。"
            ),
        )
    db_columns = get_table_columns(table_name)
    col_lens = get_table_column_lengths(table_name)
    if not db_columns:
        raise HTTPException(status_code=400, detail=f"Target table not found: {table_name}")

    mapped_df = pd.DataFrame()
    for col in db_columns:
        source_field = mapping.get(col, "")
        if isinstance(source_field, str) and source_field.startswith("__LOGIC_"):
            mapped_df[col] = ""
        elif isinstance(source_field, str) and source_field.startswith("__FIXED__:"):
            mapped_df[col] = source_field.replace("__FIXED__:", "", 1)
        elif source_field in source_df.columns:
            mapped_df[col] = source_df[source_field].astype(str)
        else:
            mapped_df[col] = ""

    if table_name == "rstran":
        mapped_df = apply_rstran_logic(mapped_df)
    elif table_name == "bw_object_name":
        mapped_df = apply_bw_object_name_logic(mapped_df)

    key_fields = get_duplicate_check_fields(table_name)
    collapsed_duplicate_rows = 0
    if table_name == "bw_object_name":
        # bw_object_name import always follows update/insert semantics.
        mapped_df, collapsed_duplicate_rows = collapse_duplicate_rows_by_keys(mapped_df, table_name, key_fields)
    elif duplicate_mode in {"continue", "update"}:
        mapped_df, collapsed_duplicate_rows = collapse_duplicate_rows_by_keys(mapped_df, table_name, key_fields)
    else:
        check_duplicates_by_mapped_columns(source_df, mapped_df, mapping, table_name, key_fields)

    # Normalize empty-like values to SQL NULL and trim text safely.
    mapped_df = mapped_df.apply(lambda col: col.map(normalize_cell))

    # Business rule: SOURCESYS is allowed to be empty for bw_object_name.
    # Some existing DB schemas still keep SOURCESYS as NOT NULL, so store empty as "" for compatibility.
    if table_name == "bw_object_name" and "SOURCESYS" in mapped_df.columns:
        mapped_df["SOURCESYS"] = mapped_df["SOURCESYS"].map(lambda v: "" if v is None else v)

    for col in db_columns:
        max_len = col_lens.get(col)
        mapped_df[col] = mapped_df[col].map(lambda v: clamp_to_length(v, max_len))

    check_missing_primary_keys(mapped_df, table_name, key_fields)

    rows = []
    for _, row in mapped_df.iterrows():
        values = []
        for col in db_columns:
            cell = row[col]
            if table_name == "bw_object_name" and col == "SOURCESYS":
                if cell is None or (isinstance(cell, str) and not cell.strip()):
                    values.append("")
                    continue
            values.append(normalize_cell(cell))
        rows.append(tuple(values))
    if not rows:
        raise HTTPException(status_code=400, detail="No rows to import")

    excel_count = len(rows)
    key_fields = get_duplicate_check_fields(table_name)
    if not key_fields:
        raise HTTPException(status_code=400, detail=f"导入失败：表 {table_name} 未配置可用于更新的键字段")

    missing_key_cols = [k for k in key_fields if k not in db_columns]
    if missing_key_cols:
        raise HTTPException(status_code=400, detail=f"导入失败：键字段不存在于目标表：{', '.join(missing_key_cols)}")

    col_sql = ", ".join(f"`{c}`" for c in db_columns)
    ph_sql = ", ".join(["%s"] * len(db_columns))
    insert_sql = f"INSERT INTO `{table_name}` ({col_sql}) VALUES ({ph_sql})"

    mutable_fields = [c for c in db_columns if c not in key_fields]
    if mutable_fields:
        set_sql = ", ".join(f"`{c}` = %s" for c in mutable_fields)
    else:
        # Fallback for key-only tables: perform a no-op update expression.
        first_key = key_fields[0]
        set_sql = f"`{first_key}` = `{first_key}`"
    where_sql = " AND ".join(f"`{k}` <=> %s" for k in key_fields)
    update_sql = f"UPDATE `{table_name}` SET {set_sql} WHERE {where_sql}"
    exists_sql = f"SELECT 1 FROM `{table_name}` WHERE {where_sql} LIMIT 1"

    conn = get_conn()
    cur = conn.cursor()
    inserted_count = 0
    updated_count = 0
    bw_object_sync_inserted = 0
    bw_object_sync_updated = 0
    try:
        row_dicts = [dict(zip(db_columns, row)) for row in rows]
        key_tuples = [tuple(row_dict[k] for k in key_fields) for row_dict in row_dicts]

        existing_keys: set[tuple] = set()
        if key_tuples:
            chunk_size = 300
            key_match_sql = " AND ".join(f"`{k}` <=> %s" for k in key_fields)
            select_cols_sql = ", ".join(f"`{k}`" for k in key_fields)

            for start in range(0, len(key_tuples), chunk_size):
                chunk = key_tuples[start:start + chunk_size]
                where_sql = " OR ".join([f"({key_match_sql})" for _ in chunk])
                params: list = []
                for key_values in chunk:
                    params.extend(key_values)
                cur.execute(
                    f"SELECT {select_cols_sql} FROM `{table_name}` WHERE {where_sql}",
                    tuple(params),
                )
                existing_keys.update(tuple(row) for row in cur.fetchall())

        update_params = []
        insert_rows = []
        for row_dict, key_values, row in zip(row_dicts, key_tuples, rows):
            if key_values in existing_keys:
                if mutable_fields:
                    update_values = [row_dict[c] for c in mutable_fields]
                    update_params.append(tuple(update_values + list(key_values)))
                else:
                    update_params.append(tuple(key_values))
            else:
                insert_rows.append(row)

        if update_params:
            cur.executemany(update_sql, update_params)
            # Keep semantics consistent with previous behavior where unchanged matched rows were counted.
            updated_count = len(update_params)

        if insert_rows:
            cur.executemany(insert_sql, insert_rows)
            inserted_count = len(insert_rows)

        if table_name == "bw_object_name":
            # Final safeguard: keep SOURCESYS normalized after import.
            cur.execute(
                """
                UPDATE `bw_object_name`
                SET `SOURCESYS` = ''
                WHERE `SOURCESYS` IS NULL
                   OR TRIM(`SOURCESYS`) = ''
                   OR CHAR_LENGTH(TRIM(`SOURCESYS`)) < 3
                """
            )
            cur.execute(
                """
                UPDATE `bw_object_name`
                SET
                  `BW_OBJECT_NORM` = UPPER(TRIM(COALESCE(`BW_OBJECT`, ''))),
                  `NAME_EN_NORM` = NULLIF(UPPER(TRIM(COALESCE(`NAME_EN`, ''))), ''),
                  `NAME_DE_NORM` = NULLIF(UPPER(TRIM(COALESCE(`NAME_DE`, ''))), '')
                """
            )

        if table_name == "rstran":
            sync_stats = sync_bw_object_name_from_rstran(cur)
            bw_object_sync_inserted = int(sync_stats.get("inserted", 0))
            bw_object_sync_updated = int(sync_stats.get("updated", 0))
            cur.execute(
                """
                UPDATE `bw_object_name`
                SET
                  `BW_OBJECT_NORM` = UPPER(TRIM(COALESCE(`BW_OBJECT`, ''))),
                  `NAME_EN_NORM` = NULLIF(UPPER(TRIM(COALESCE(`NAME_EN`, ''))), ''),
                  `NAME_DE_NORM` = NULLIF(UPPER(TRIM(COALESCE(`NAME_DE`, ''))), '')
                """
            )

        cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        db_count = int(cur.fetchone()[0])

        conn.commit()
        affected = inserted_count + updated_count
    except HTTPException:
        cur.close()
        conn.close()
        raise
    except mysql.connector.Error as exc:
        conn.rollback()
        detail = "导入失败，已回滚到导入前数据。请检查Excel是否有键字段重复或字段异常后重新导入。"
        if exc.errno == 1062:
            detail = "导入失败：检测到重复键值数据。已回滚到导入前数据，请去重后重新导入。"
        elif exc.errno == 1048:
            exc_text = str(exc)
            col_match = re.search(r"Column\s+'([^']+)'\s+cannot\s+be\s+null", exc_text, flags=re.IGNORECASE)
            col_name = col_match.group(1) if col_match else "未知字段"
            detail = f"导入失败：字段 {col_name} 不能为 NULL（当前数据库表结构限制）。请检查表结构或改为空字符串后重试。"
        raise HTTPException(status_code=400, detail=detail) from exc
    finally:
        cur.close()
        conn.close()

    status = upsert_status(table_name, count_table_rows(table_name))
    return {
        "table_name": table_name,
        "affected_rows": int(affected),
        "inserted_rows": int(inserted_count),
        "updated_rows": int(updated_count),
        "collapsed_duplicate_rows": int(collapsed_duplicate_rows),
        "excel_count": int(excel_count),
        "db_count": int(db_count),
        "bw_object_sync_inserted": int(bw_object_sync_inserted),
        "bw_object_sync_updated": int(bw_object_sync_updated),
        "last_update": status["last_update"],
        "last_count": status["last_count"],
        "message": "Import completed",
    }


@app.post("/api/import/clear-table")
def clear_import_table(table_name: str = Form(...)) -> Dict[str, str | int]:
    table_name = str(table_name or "").strip()
    if table_name != "rstran":
        raise HTTPException(status_code=400, detail="Only rstran supports clear-table in current workflow")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM `rstran`")
        cur.execute("SELECT COUNT(*) FROM `rstran`")
        db_count = int(cur.fetchone()[0])
        conn.commit()
    except mysql.connector.Error as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail="删除失败：数据库执行异常") from exc
    finally:
        cur.close()
        conn.close()

    status = upsert_status("rstran", db_count)
    return {
        "table_name": "rstran",
        "db_count": int(db_count),
        "last_update": status["last_update"],
        "last_count": status["last_count"],
        "message": "Clear completed",
    }
