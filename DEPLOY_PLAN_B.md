# Deployment Plan B (Cloudflare Pages + Render + TiDB Cloud)

This guide is for long-running free validation without Oracle Cloud.

## 0) Architecture

- Frontend: Cloudflare Pages (static)
- Backend API: Render Web Service (Docker)
- Database: TiDB Cloud Serverless (MySQL-compatible)

## 1) What to Deploy (Whitelist)

### Frontend (Cloudflare Pages)
Deploy only the `frontend-prototype` directory output.

Required files/folders under `frontend-prototype`:
- `index.html`
- `flow.html`
- `styles.css`
- `app.js`
- `flow-page.js`
- `mock-data.js`
- `runtime-config.js`
- `Assets/Icons/*`

### Backend (Render)
Required files:
- `backend/import_status_api.py`
- `backend/requirements.txt`
- `backend/entrypoint.sh`
- `backend/Dockerfile`
- `scripts/create_rstran_table.py`
- `scripts/create_bw_object_name_table.py`

## 2) Create TiDB Cloud Serverless

1. Create a TiDB Cloud account and a Serverless cluster.
2. Create one database:
   - `dataflow_digram`
3. Create one SQL user and password.
4. Add your Render outbound access (or allow all temporarily for validation).
   - TiDB public endpoint requires TLS.
5. Collect these values:
   - host
   - port
   - username
   - password
   - database name (`dataflow_digram`)

## 3) Deploy Backend on Render

1. Push this repository to GitHub.
2. In Render, create a new Web Service from your repo.
3. Choose Docker deployment.
4. Set:
   - Dockerfile path: `backend/Dockerfile`
   - Docker context: repository root (`.`)
5. Add environment variables:

```bash
DB_HOST=<tidb_host>
DB_PORT=<tidb_port>
DB_USER=<tidb_user>
DB_PASSWORD=<tidb_password>
DB_NAME=dataflow_digram
DB_SSL_CA=/etc/ssl/cert.pem
DB_SSL_DISABLED=false
DB_SSL_VERIFY_CERT=true
DB_SSL_VERIFY_IDENTITY=true

DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<strong_admin_password>

# Cross-origin for Pages production domain
CORS_ALLOW_ORIGINS=https://<your-pages-project>.pages.dev

# Optional for preview domains:
# CORS_ALLOW_ORIGIN_REGEX=https://.*\.pages\.dev

# Required for cross-site cookie between Pages and Render
AUTH_COOKIE_SECURE=true
AUTH_COOKIE_SAMESITE=none
AUTH_COOKIE_DOMAIN=
```

6. Deploy and copy your Render URL, e.g.:
   - `https://dataflow-api.onrender.com`

## 4) Deploy Frontend on Cloudflare Pages

1. In Cloudflare Pages, connect the same GitHub repo.
2. Set build settings:
   - Build command: (empty)
   - Output directory: `frontend-prototype`
3. Deploy.
4. After first deploy, edit this file in repo:
   - `frontend-prototype/runtime-config.js`

Set:

```js
window.__DATAFLOW_API_BASE__ = "https://dataflow-api.onrender.com";
```

Commit and let Pages redeploy.

## 5) Verification Checklist

1. Open `https://<your-pages-project>.pages.dev`
2. Login with admin account.
3. Run search and open flow page.
4. Confirm API calls include cookies and return 200.

## 6) Common Issues

### Login succeeds but later calls return 401

Check on Render:
- `AUTH_COOKIE_SECURE=true`
- `AUTH_COOKIE_SAMESITE=none`
- `CORS_ALLOW_ORIGINS` includes the exact Pages domain
- Backend response includes `Access-Control-Allow-Credentials: true`

### CORS blocked in browser

- Ensure frontend origin is included exactly in `CORS_ALLOW_ORIGINS`.
- If using Pages preview URLs, use `CORS_ALLOW_ORIGIN_REGEX=https://.*\.pages\.dev`.

### Render cold start

- Free plan may sleep; first request can be slow.
- This is expected in free-tier validation.

## 7) Daily Operations

- Update code: push to GitHub
- Backend: Render auto-redeploy
- Frontend: Pages auto-redeploy
- Database backup: use TiDB export/snapshot tools
