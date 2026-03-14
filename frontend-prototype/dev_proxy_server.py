from __future__ import annotations

import http.server
import os
import socketserver
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


FRONTEND_DIR = Path(__file__).resolve().parent
BACKEND_BASE = os.environ.get("DATAFLOW_BACKEND_BASE", "https://dataflow-api-production.up.railway.app").rstrip("/")
PORT = int(os.environ.get("DATAFLOW_DEV_PORT", "8088"))


class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(FRONTEND_DIR), **kwargs)

    def do_OPTIONS(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        super().do_OPTIONS()

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html" or self.path.startswith("/index.html?"):
            index_path = FRONTEND_DIR / "index.html"
            html = index_path.read_text(encoding="utf-8")
            html = html.replace(
                '<script src="./runtime-config.js?v=20260314-mapfix18"></script>',
                (
                    '<script>window.__DATAFLOW_API_BASE__ = "http://localhost:'
                    f'{PORT}"; window.__DATAFLOW_APP_VERSION__ = "1.0.1-local";</script>'
                ),
            )
            html = html.replace("./app.js?v=20260314-mapfix18", f"./app.js?v=localproxy-{PORT}")
            html = html.replace("./styles.css?v=20260314-mapfix18", f"./styles.css?v=localproxy-{PORT}")
            html = html.replace("./mock-data.js?v=20260314-mapfix18", f"./mock-data.js?v=localproxy-{PORT}")
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/runtime-config.js" or self.path.startswith("/runtime-config.js?"):
            body = (
                f'window.__DATAFLOW_API_BASE__ = "http://localhost:{PORT}";\n'
                'window.__DATAFLOW_APP_VERSION__ = "1.0.1-local";\n'
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path.startswith("/api/"):
            self._proxy_request()
            return

        super().do_GET()

    def do_POST(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def do_PUT(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def do_PATCH(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def do_DELETE(self):
        if self.path.startswith("/api/"):
            self._proxy_request()
            return
        self.send_error(405, "Method not allowed")

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _proxy_request(self):
        target_url = f"{BACKEND_BASE}{self.path}"
        body = None
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length > 0:
            body = self.rfile.read(content_length)

        req = urllib.request.Request(target_url, data=body, method=self.command)

        for key, value in self.headers.items():
            lower = key.lower()
            if lower in {"host", "origin", "referer", "content-length", "connection"}:
                continue
            req.add_header(key, value)

        req.add_header("Origin", BACKEND_BASE)

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = resp.read()
                self.send_response(resp.status)
                for key, value in resp.headers.items():
                    lower = key.lower()
                    if lower in {"content-length", "transfer-encoding", "connection", "content-encoding", "access-control-allow-origin", "access-control-allow-credentials", "set-cookie"}:
                        continue
                    self.send_header(key, value)

                cookies = resp.headers.get_all("Set-Cookie") or []
                for cookie in cookies:
                    self.send_header("Set-Cookie", self._rewrite_cookie(cookie))

                self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
                self.send_header("Access-Control-Allow-Credentials", "true")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            self.send_response(exc.code)
            for key, value in exc.headers.items():
                lower = key.lower()
                if lower in {"content-length", "transfer-encoding", "connection", "content-encoding", "access-control-allow-origin", "access-control-allow-credentials", "set-cookie"}:
                    continue
                self.send_header(key, value)
            cookies = exc.headers.get_all("Set-Cookie") or []
            for cookie in cookies:
                self.send_header("Set-Cookie", self._rewrite_cookie(cookie))
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as exc:  # pragma: no cover - local helper path
            payload = str(exc).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", f"http://localhost:{PORT}")
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    @staticmethod
    def _rewrite_cookie(cookie: str) -> str:
        updated = cookie.replace("; Secure", "")
        updated = updated.replace("; SameSite=none", "; SameSite=Lax")
        updated = updated.replace("; SameSite=None", "; SameSite=Lax")
        return updated


class ThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


if __name__ == "__main__":
    with ThreadingTCPServer(("127.0.0.1", PORT), ProxyHandler) as httpd:
        print(f"Serving local frontend proxy at http://localhost:{PORT}")
        print(f"Proxying /api to {BACKEND_BASE}")
        httpd.serve_forever()