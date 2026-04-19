"""HTTP server for Prometheus metrics, health checks, status API, and web UI."""

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Health state (unchanged)
# ---------------------------------------------------------------------------


class _HealthState:
    """Tracks recency of poll and replication for health checks.

    Thread-safe: the main loop writes state, the HTTP handler thread reads it.
    """

    def __init__(self, max_poll_age_s: float = 300, max_replication_age_s: float = 600):
        self.max_poll_age_s = max_poll_age_s
        self.max_replication_age_s = max_replication_age_s
        self._lock = threading.Lock()
        self._last_poll: float = 0
        self._last_replication: float = 0
        self._started: bool = False
        self._has_replicated: bool = False

    def record_poll(self) -> None:
        with self._lock:
            self._last_poll = time.monotonic()

    def record_replication(self) -> None:
        with self._lock:
            self._last_replication = time.monotonic()
            self._has_replicated = True

    def mark_started(self) -> None:
        with self._lock:
            now = time.monotonic()
            self._last_poll = now
            self._started = True

    def is_alive(self) -> bool:
        with self._lock:
            if not self._started:
                return False
            return (time.monotonic() - self._last_poll) < self.max_poll_age_s

    def is_ready(self) -> bool:
        with self._lock:
            if not self._started:
                return False
            if (time.monotonic() - self._last_poll) >= self.max_poll_age_s:
                return False
            if self._has_replicated:
                return (time.monotonic() - self._last_replication) < self.max_replication_age_s
            return True

    def status_body(self) -> str:
        with self._lock:
            now = time.monotonic()
            poll_age = f"{now - self._last_poll:.1f}s ago" if self._started else "never"
            repl_age = f"{now - self._last_replication:.1f}s ago" if self._has_replicated else "never"
        return f"poll={poll_age} replication={repl_age}"


health = _HealthState()


# ---------------------------------------------------------------------------
# Status snapshot (new — for /status and /ui)
# ---------------------------------------------------------------------------


@dataclass
class DestStatus:
    id: str
    routing_value: str
    snapshot: int
    lag: int
    rows_replicated: int
    status: str  # "healthy", "lagging", "error"
    last_error: str | None


class StatusState:
    """Thread-safe snapshot of replication state for the status API and web UI."""

    def __init__(self):
        self._lock = threading.Lock()
        self._data: dict | None = None
        self._started_at: float = time.monotonic()

    def update(
        self,
        *,
        source_table: str,
        source_snapshot: int | None,
        mode: str,
        poll_interval: float,
        destinations: list[DestStatus],
        pool_open: int,
        pool_max: int,
    ) -> None:
        with self._lock:
            self._data = {
                "source_table": source_table,
                "source_snapshot": source_snapshot,
                "mode": mode,
                "poll_interval": poll_interval,
                "uptime_s": round(time.monotonic() - self._started_at, 1),
                "destinations": [asdict(d) for d in destinations],
                "pool": {"open": pool_open, "max": pool_max},
            }

    def snapshot(self) -> dict:
        with self._lock:
            if self._data is None:
                return {
                    "source_table": None,
                    "source_snapshot": None,
                    "mode": None,
                    "poll_interval": None,
                    "uptime_s": round(time.monotonic() - self._started_at, 1),
                    "destinations": [],
                    "pool": {"open": 0, "max": 0},
                }
            # Update uptime on every read
            data = dict(self._data)
            data["uptime_s"] = round(time.monotonic() - self._started_at, 1)
            return data

    def to_json(self) -> str:
        return json.dumps(self.snapshot())


status = StatusState()


# ---------------------------------------------------------------------------
# HTML dashboard (inline, no external deps)
# ---------------------------------------------------------------------------

_UI_HTML = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Viaduck Status</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
         margin: 2em; background: #fafafa; color: #333; }
  h1 { font-size: 1.4em; margin-bottom: 0.2em; }
  .meta { color: #666; font-size: 0.9em; margin-bottom: 1.5em; }
  table { border-collapse: collapse; width: 100%; max-width: 900px; }
  th, td { text-align: left; padding: 6px 12px; border-bottom: 1px solid #ddd; }
  th { background: #f0f0f0; font-weight: 600; }
  .healthy { color: #2e7d32; }
  .lagging { color: #f57f17; }
  .error { color: #c62828; font-weight: bold; }
  .pool { margin-top: 1.5em; font-size: 0.9em; color: #666; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
         margin-right: 6px; vertical-align: middle; }
  .dot-healthy { background: #4caf50; }
  .dot-lagging { background: #ffb300; }
  .dot-error { background: #e53935; }
  #disconnected { display: none; color: #c62828; font-size: 0.9em; margin-top: 1em; }
</style>
</head>
<body>
<h1>Viaduck</h1>
<div class="meta" id="meta">Connecting...</div>
<table>
  <thead>
    <tr><th>Destination</th><th>Routing Value</th><th>Snapshot</th><th>Lag</th>
        <th>Rows</th><th>Status</th><th>Last Error</th></tr>
  </thead>
  <tbody id="tbody"></tbody>
</table>
<div class="pool" id="pool"></div>
<div id="disconnected">SSE disconnected. Reconnecting...</div>
<script>
const tbody = document.getElementById('tbody');
const meta = document.getElementById('meta');
const pool = document.getElementById('pool');
const disc = document.getElementById('disconnected');

function fmt(n) { return n != null ? n.toLocaleString() : '-'; }

function update(d) {
  meta.textContent = 'Source: ' + (d.source_table || '?') +
    ' @ snapshot ' + fmt(d.source_snapshot) +
    '  |  Mode: ' + (d.mode || '?') +
    '  |  Poll: ' + (d.poll_interval || '?') + 's' +
    '  |  Uptime: ' + Math.round((d.uptime_s || 0) / 60) + 'm';

  let html = '';
  (d.destinations || []).forEach(function(dest) {
    const cls = dest.status || 'healthy';
    html += '<tr>' +
      '<td><span class="dot dot-' + cls + '"></span>' + dest.id + '</td>' +
      '<td>' + dest.routing_value + '</td>' +
      '<td>' + fmt(dest.snapshot) + '</td>' +
      '<td>' + fmt(dest.lag) + '</td>' +
      '<td>' + fmt(dest.rows_replicated) + '</td>' +
      '<td class="' + cls + '">' + cls + '</td>' +
      '<td>' + (dest.last_error || '') + '</td></tr>';
  });
  tbody.innerHTML = html;

  const p = d.pool || {};
  pool.textContent = 'Pool: ' + (p.open || 0) + '/' + (p.max || 0) + ' open';
}

function connect() {
  const es = new EventSource('/ui/sse');
  es.onmessage = function(e) { disc.style.display = 'none'; update(JSON.parse(e.data)); };
  es.onerror = function() { disc.style.display = 'block'; };
}
connect();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

_web_enabled = True


def _make_handler():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                payload = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif self.path == "/healthz":
                body = health.status_body()
                if health.is_alive():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(f"ok {body}\n".encode())
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(f"unhealthy {body}\n".encode())
            elif self.path == "/readyz":
                body = health.status_body()
                if health.is_ready():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(f"ok {body}\n".encode())
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(f"not ready {body}\n".encode())
            elif self.path == "/status" and _web_enabled:
                payload = status.to_json().encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif self.path == "/ui" and _web_enabled:
                payload = _UI_HTML.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            elif self.path == "/ui/sse" and _web_enabled:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                try:
                    while True:
                        data = status.to_json()
                        self.wfile.write(f"data: {data}\n\n".encode())
                        self.wfile.flush()
                        time.sleep(2)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # client disconnected
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass

    return Handler


def start(port: int = 8000, web_enabled: bool = True) -> HTTPServer:
    global _web_enabled
    _web_enabled = web_enabled
    server = HTTPServer(("", port), _make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoints = "/metrics, /healthz, /readyz"
    if web_enabled:
        endpoints += ", /status, /ui"
    log.info("HTTP server listening on port %d (%s)", port, endpoints)
    return server
