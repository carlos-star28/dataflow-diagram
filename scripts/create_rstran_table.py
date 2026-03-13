import pandas as pd
import mysql.connector
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[1]
EXCEL_PATH = BASE_DIR / "Table-Template" / "table_definition_rstran.xlsx"
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "showlang")
DB_NAME = os.getenv("DB_NAME", "dataflow_digram")
TABLE_NAME = "rstran"


def build_columns(frame: pd.DataFrame):
    columns = []
    primary_keys = []

    for _, row in frame.iterrows():
        field = str(row["Field"]).strip()
        dtype = str(row["Data type"]).strip().lower()
        length = int(row["Len"])
        decimals = int(row["Decimals"]) if not pd.isna(row["Decimals"]) else 0
        comment = str(row["Field Text"]).replace("'", "''")

        if dtype == "varchar":
            col_type = f"VARCHAR({length})"
        elif dtype in {"char", "nchar"}:
            col_type = f"CHAR({length})"
        elif dtype in {"int", "integer"}:
            col_type = "INT"
        elif dtype in {"bigint"}:
            col_type = "BIGINT"
        elif dtype in {"decimal", "number", "numeric"}:
            col_type = f"DECIMAL({length},{decimals})"
        elif dtype == "date":
            col_type = "DATE"
        elif dtype in {"datetime", "timestamp"}:
            col_type = "DATETIME"
        else:
            col_type = f"VARCHAR({length})"

        is_key = str(row.get("KEY", "")).strip().upper() == "KEY"
        null_sql = "NOT NULL" if is_key else "NULL"
        columns.append(f"  `{field}` {col_type} {null_sql} COMMENT '{comment}'")
        if is_key:
            primary_keys.append(f"`{field}`")

    pk_sql = f",\n  PRIMARY KEY ({', '.join(primary_keys)})" if primary_keys else ""
    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{DB_NAME}`.`{TABLE_NAME}` (\n"
        + ",\n".join(columns)
        + pk_sql
        + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    )

    return ddl


def main():
    frame = pd.read_excel(str(EXCEL_PATH))
    ddl = build_columns(frame)

    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4")
    cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Created table: {DB_NAME}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
