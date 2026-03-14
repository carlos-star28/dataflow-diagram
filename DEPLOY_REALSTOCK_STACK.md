# Dataflow Digram 同 realstock 技术栈部署说明

目标：与 realstock 保持同一技术栈
- 前端：Netlify
- 后端：Railway（Docker）
- 数据库：TiDB Cloud（MySQL 兼容）

本说明默认你已将仓库推送到 GitHub。

## 1. 对齐检查结论（已完成）

和 realstock 对比后，当前项目满足同栈部署前提：
- 前端是纯静态资源目录：`frontend-prototype`
- 后端是 FastAPI + Docker，可直接交给 Railway 运行
- 已支持 TiDB TLS 参数（`DB_SSL_*`）
- 已支持跨域 Cookie 与 CORS 环境变量

为同栈部署新增文件：
- `netlify.toml`（前端发布目录）
- `Dockerfile`（根目录 Docker，便于 Railway 自动识别）

并调整：
- `frontend-prototype/runtime-config.js` 默认 API 地址改为空，避免继续指向旧 Render 地址

## 2. 部署后端到 Railway

1) 登录 Railway，选择 New Project -> Deploy from GitHub Repo。

2) 选择本仓库后，Railway 会自动识别根目录 `Dockerfile`。

3) 在 Railway Service 环境变量中配置：

```bash
DB_HOST=<你的_tidb_host>
DB_PORT=4000
DB_USER=<你的_tidb_user>
DB_PASSWORD=<你的_tidb_password>
DB_NAME=dataflow_digram

DB_SSL_CA=/etc/ssl/certs/ca-certificates.crt
DB_SSL_DISABLED=false
DB_SSL_VERIFY_CERT=true
DB_SSL_VERIFY_IDENTITY=true

DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<强密码>

# 先留空，拿到 Netlify 地址后再补
CORS_ALLOW_ORIGINS=
CORS_ALLOW_ORIGIN_REGEX=

AUTH_COOKIE_NAME=df_session
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=
```

4) 部署完成后，记录 Railway 公开地址（例如 `https://dataflow-api-production.up.railway.app`）。

5) 验证后端健康：
- 打开 `https://你的-railway-域名/docs`，应看到 FastAPI 文档页。

## 3. 部署前端到 Netlify

1) 登录 Netlify，Add new site -> Import an existing project。

2) 连接同一个 GitHub 仓库。

3) 由于已存在 `netlify.toml`，Netlify 会自动使用：
- Publish directory: `frontend-prototype`
- Build command: 空

4) 完成首次部署后，获取 Netlify 域名（例如 `https://dataflow-digram.netlify.app`）。

## 4. 回填跨域与 API 地址

1) 回到 Railway，更新：

```bash
CORS_ALLOW_ORIGINS=https://你的-netlify-域名
# 如果你需要 Netlify 分支预览域名，可额外使用：
# CORS_ALLOW_ORIGIN_REGEX=https://.*--dataflow-digram\.netlify\.app
```

2) 修改 `frontend-prototype/runtime-config.js`：

```js
window.__DATAFLOW_API_BASE__ = "https://你的-railway-域名";
```

3) 提交后触发 Netlify 自动重部署。

## 5. 联调验收清单

按顺序检查：
- 前端首页可打开，版本角标显示正常
- 可登录（默认管理员或你配置的管理员）
- 登录后请求携带 Cookie，且返回 200
- 搜索与数据流图页面可用
- 导入 Excel 可正常写入数据库
- `https://你的-railway-域名/docs` 可访问

建议重点查看浏览器 Network：
- `Access-Control-Allow-Origin` 是否精确等于 Netlify 域名
- `Access-Control-Allow-Credentials: true`
- Cookie 是否带 `Secure` + `SameSite=None`

## 6. 常见问题快速定位

1) 登录成功但后续 401
- 检查 `AUTH_COOKIE_SECURE=true`
- 检查 `AUTH_COOKIE_SAMESITE=none`
- 检查 `CORS_ALLOW_ORIGINS` 精确匹配 Netlify 域名（含协议）

2) 浏览器报 CORS
- Railway 端 `CORS_ALLOW_ORIGINS` 未配置或配置错误
- 若使用预览地址，补 `CORS_ALLOW_ORIGIN_REGEX`

3) Railway 启动失败（数据库连接）
- TiDB 主机/端口/用户密码错误
- TLS 配置错误，优先确认 `DB_SSL_CA` 和 `DB_SSL_VERIFY_*`

4) 导入功能异常
- 检查 Railway 日志是否出现 `create_rstran_table` 或 `create_bw_object_name_table` 报错

## 7. 与 realstock 一致性映射

realstock：
- 前端 Netlify（`netlify.toml` + 子目录发布）
- 后端 Railway（Docker）

Dataflow Digram（本次对齐后）：
- 前端 Netlify（根目录 `netlify.toml`，发布 `frontend-prototype`）
- 后端 Railway（根目录 `Dockerfile`）
- 数据库 TiDB（沿用现有 `DB_SSL_*` 变量）

到此，两个项目已使用同一部署技术栈和相同的发布分层方式。