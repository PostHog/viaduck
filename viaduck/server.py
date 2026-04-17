"""HTTP server for Prometheus metrics and health checks."""

import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


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
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass

    return Handler


def start(port: int = 8000) -> HTTPServer:
    server = HTTPServer(("", port), _make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("HTTP server listening on port %d (/metrics, /healthz, /readyz)", port)
    return server
