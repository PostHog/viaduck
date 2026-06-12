"""HTTP server for Prometheus metrics, health checks, status API, and web UI."""

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Health state (unchanged)
# ---------------------------------------------------------------------------


class _HealthState:
    """Tracks recency of poll and replication for health checks.

    Thread-safe: the main loop writes state, the HTTP handler thread reads it.
    """

    def __init__(self, max_poll_age_s: float = 300):
        self.max_poll_age_s = max_poll_age_s
        self._lock = threading.Lock()
        self._last_poll: float = 0
        # `None` means "never replicated"; using a sentinel rather than 0.0
        # because `time.monotonic()` is permitted to return 0.0 (and tests
        # in this repo drive a virtual clock that starts at 0), so a real
        # replication recorded at t=0 must not be misreported as "never".
        self._last_replication: float | None = None
        self._started: bool = False

    def record_poll(self) -> None:
        with self._lock:
            self._last_poll = time.monotonic()

    def record_replication(self) -> None:
        # Tracked only for diagnostics in `status_body`; not gated on by
        # `is_ready`. An idle source legitimately produces long stretches
        # with no writes, and gating readiness on write recency caused
        # healthy pods to flap to NotReady whenever the source went quiet.
        with self._lock:
            self._last_replication = time.monotonic()

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
            return (time.monotonic() - self._last_poll) < self.max_poll_age_s

    def status_body(self) -> str:
        with self._lock:
            now = time.monotonic()
            poll_age = f"{now - self._last_poll:.1f}s ago" if self._started else "never"
            repl_age = f"{now - self._last_replication:.1f}s ago" if self._last_replication is not None else "never"
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
    status: str  # "healthy", "buffering", "flushing", "lagging", "error"
    last_error: str | None
    buffer_rows: int = 0
    buffer_age_s: float = 0.0
    # This-run cumulative counters (reset on restart): applied counts are
    # pre-Phase-2 change types of successfully flushed batches.
    applied_inserts: int = 0
    applied_updates: int = 0
    applied_deletes: int = 0
    buffered_rows_total: int = 0
    lag_seconds: float = 0.0


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
        flush_interval: float = 0.0,
        delivery_config: dict | None = None,
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
                "flush_interval": flush_interval,
                "delivery_config": delivery_config or {},
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
                    "flush_interval": None,
                    "delivery_config": {},
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
  table { border-collapse: collapse; width: 100%; max-width: 1100px; }
  th, td { text-align: left; padding: 6px 12px; border-bottom: 1px solid #ddd; }
  th { background: #f0f0f0; font-weight: 600; cursor: help;
       white-space: nowrap; vertical-align: bottom; }
  .unit { display: block; font-weight: 400; font-size: 0.75em; color: #888; }
  .sort-ind { font-size: 0.75em; color: #555; margin-left: 4px; }
  .lag-warn { color: #f57f17; font-weight: 600; }
  .lag-bad { color: #c62828; font-weight: 600; }
  th[data-sort] { cursor: pointer; user-select: none; }
  /* numeric columns: Snapshot, Lag, Rows Processed, Buffered */
  th:nth-child(n+3):nth-child(-n+7), td:nth-child(n+3):nth-child(-n+7) { text-align: right; }
  td:nth-child(8) { cursor: help; }
  .tip { cursor: help; border-bottom: 1px dotted #999; }
  .healthy { color: #2e7d32; }
  .buffering { color: #1565c0; }
  .flushing { color: #00838f; }
  .lagging { color: #f57f17; }
  .error { color: #c62828; font-weight: bold; }
  .pool { margin-top: 1.5em; font-size: 0.9em; color: #666; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
         margin-right: 6px; vertical-align: middle; }
  .dot-healthy { background: #4caf50; }
  .dot-buffering { background: #42a5f5; }
  .dot-flushing { background: #26c6da; }
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
    <tr>
      <th title="Destination id from the config" data-sort="id"
data-type="str">Destination<span class="sort-ind"></span></th>
      <th title="Source rows whose routing field equals this value are delivered here"
data-sort="routing_value" data-type="str">Routing Value<span class="sort-ind"></span></th>
      <th title="Last source snapshot durably flushed to this destination (the persisted cursor)"
data-sort="snapshot" data-type="num">Snapshot<span class="sort-ind"></span></th>
      <th title="Age of the oldest position advance not yet flushed durable — how far the
persisted cursor trails in wall-clock terms. Anything under the flush interval is the
buffering design working. Hover a cell for the snapshot count"
data-sort="lag_seconds" data-type="num">Lag<span class="sort-ind"></span><span
class="unit">seconds behind</span></th>
      <th title="Cumulative operations applied since seeding (upserts + deletes),
not the destination's current row count" data-sort="rows_replicated"
data-type="num">Rows Processed<span class="sort-ind"></span><span class="unit">ops applied</span></th>
      <th title="Rows read from the source and awaiting flush (buffer age in seconds).
Hover a cell for the cumulative rows buffered this run"
data-sort="buffer_rows" data-type="num">Buffered<span class="sort-ind"></span><span
class="unit">rows (age)</span></th>
      <th title="Rows forwarded to the destination this run, counted before conflict
resolution (+ inserts / Δ updates / − deletes). In-memory: resets on restart;
at-least-once replays can double-count" data-sort="applied_total"
data-type="num">Applied<span class="sort-ind"></span><span
class="unit">+ / Δ / −</span></th>
      <th title="healthy: flushed through the current snapshot. buffering: read-current,
data awaiting flush (normal). flushing: a flush is in progress. lagging: reads are
behind the source. error: last flush failed; the range will be re-read"
data-sort="status" data-type="str">Status<span class="sort-ind"></span></th>
      <th title="Most recent flush error; cleared on the next successful flush"
data-sort="last_error" data-type="str">Last Error<span class="sort-ind"></span></th>
    </tr>
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

const modeTips = {
  full_cdc: 'key_columns configured: inserts, updates, and deletes replicate via atomic delete+upsert.',
  append_only: 'No key_columns: inserts only, replicated via append. Deletes and updates are not captured.',
};

const statusTips = {
  healthy: 'Flushed through the current source snapshot — nothing pending.',
  buffering: 'Reads are current; changes are sitting in the in-memory buffer awaiting the next flush. ' +
    'Normal operation between flushes.',
  flushing: 'A flush is writing this destination right now.',
  lagging: 'CDC reads are behind the source — there are changes viaduck has not yet read.',
  error: 'The last flush failed; the buffered range was dropped and will be re-read from the persisted cursor. ' +
    'See Last Error.',
};

function esc(x) {
  return String(x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

let sortKey = null;
let sortDir = 1;  // 1 asc, -1 desc
let lastData = null;

function fmtBytes(n) {
  if (n == null) return '?';
  if (n >= 1073741824) return (n / 1073741824).toFixed(n % 1073741824 ? 1 : 0) + ' GiB';
  if (n >= 1048576) return Math.round(n / 1048576) + ' MiB';
  return fmt(n) + ' B';
}

function flushTip(d) {
  const c = d.delivery_config || {};
  return 'Changes buffer in memory and flush on the FIRST trigger to fire: ' +
    'age \u2265 ' + esc(d.flush_interval || '?') + 's \u00b7 ' +
    'rows \u2265 ' + fmt(c.flush_max_rows) + ' \u00b7 ' +
    'bytes \u2265 ' + fmtBytes(c.flush_max_bytes) + ' \u00b7 ' +
    'global watermark ' + fmtBytes(c.buffer_total_max_bytes) + ' (largest first) \u00b7 ' +
    'shutdown. Workers: ' + fmt(c.workers) + ', connection pool: ' + fmt(c.pool_max_open);
}

function lagClass(dest, d) {
  // Within one flush interval is the buffering design working; past it,
  // something is slow; past two, something is stuck.
  const fi = d.flush_interval || 0;
  if (!fi || dest.lag_seconds <= fi) return '';
  return dest.lag_seconds > 2 * fi ? 'lag-bad' : 'lag-warn';
}

function sortVal(dest, key) {
  if (key === 'applied_total') {
    return dest.applied_inserts + dest.applied_updates + dest.applied_deletes;
  }
  return dest[key];
}

document.querySelectorAll('th[data-sort]').forEach(function(th) {
  th.addEventListener('click', function() {
    const key = th.dataset.sort;
    if (sortKey === key) {
      sortDir = -sortDir;
    } else {
      sortKey = key;
      // numbers feel right descending first; strings ascending
      sortDir = th.dataset.type === 'num' ? -1 : 1;
    }
    document.querySelectorAll('.sort-ind').forEach(function(el) { el.textContent = ''; });
    th.querySelector('.sort-ind').textContent = sortDir === 1 ? '\u25b2' : '\u25bc';
    if (lastData) update(lastData);
  });
});

function update(d) {
  lastData = d;
  meta.innerHTML = 'Source: ' + esc(d.source_table || '?') +
    ' @ snapshot ' + fmt(d.source_snapshot) +
    '  |  Mode: <span class="tip" title="' + esc(modeTips[d.mode] || '') + '">' + esc(d.mode || '?') + '</span>' +
    '  |  Poll: every ' + esc(d.poll_interval || '?') + 's' +
    '  |  <span class="tip" title="' + flushTip(d) + '">Flush: every ' + esc(d.flush_interval || '?') + 's</span>' +
    '  |  Uptime: ' + Math.round((d.uptime_s || 0) / 60) + 'm';

  let html = '';
  let dests = (d.destinations || []).slice();
  if (sortKey) {
    dests.sort(function(a, b) {
      const av = sortVal(a, sortKey), bv = sortVal(b, sortKey);
      if (av === bv) return a.id < b.id ? -1 : 1;  // stable-ish tiebreak
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      return (av < bv ? -1 : 1) * sortDir;
    });
  }
  dests.forEach(function(dest) {
    const cls = dest.status || 'healthy';
    html += '<tr>' +
      '<td><span class="dot dot-' + cls + '"></span>' + dest.id + '</td>' +
      '<td>' + dest.routing_value + '</td>' +
      '<td>' + fmt(dest.snapshot) + '</td>' +
      '<td title="' + fmt(dest.lag) + ' snapshots behind" class="' + lagClass(dest, d) + '">' +
        (dest.lag_seconds >= 0.5 ? Math.round(dest.lag_seconds) + 's' : '-') + '</td>' +
      '<td>' + fmt(dest.rows_replicated) + '</td>' +
      '<td title="Cumulative rows buffered this run: ' + fmt(dest.buffered_rows_total) + '">' +
        (dest.buffer_rows
        ? fmt(dest.buffer_rows) + ' (' + Math.round(dest.buffer_age_s) + 's)' : '-') + '</td>' +
      '<td>+' + fmt(dest.applied_inserts) + ' \u0394' + fmt(dest.applied_updates) +
        ' \u2212' + fmt(dest.applied_deletes) + '</td>' +
      '<td class="' + cls + '" title="' + (statusTips[cls] || '') + '">' + cls + '</td>' +
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
# Set when the server is shutting down so long-lived handlers (SSE) can exit
# their loops promptly. Without this, `http.shutdown()` blocks forever on any
# active /ui/sse client.
_shutdown_event = threading.Event()


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
                self.send_header("X-Accel-Buffering", "no")
                self.end_headers()
                try:
                    # Loop exits on shutdown signal, client disconnect, or
                    # any I/O error. Using `_shutdown_event.wait` instead of
                    # raw `time.sleep` so server.shutdown() unblocks
                    # immediately on terminate.
                    while not _shutdown_event.is_set():
                        data = status.to_json()
                        self.wfile.write(f"data: {data}\n\n".encode())
                        self.wfile.flush()
                        if _shutdown_event.wait(timeout=2.0):
                            break
                except (BrokenPipeError, ConnectionResetError):
                    pass  # client disconnected
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass

    return Handler


def signal_shutdown() -> None:
    """Tell long-lived handlers (SSE) to exit their loops.

    Call this before `server.shutdown()` so any active /ui/sse client
    releases its handler thread and shutdown() doesn't block.
    """
    _shutdown_event.set()


def start(port: int = 8000, web_enabled: bool = True) -> ThreadingHTTPServer:
    global _web_enabled
    _web_enabled = web_enabled
    # ThreadingHTTPServer (not HTTPServer) so a long-lived /ui/sse client
    # can't block /healthz, /readyz, or /metrics. With single-threaded
    # HTTPServer, k8s liveness probes time out under any UI traffic and
    # the pod gets killed.
    server = ThreadingHTTPServer(("", port), _make_handler())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    endpoints = "/metrics, /healthz, /readyz"
    if web_enabled:
        endpoints += ", /status, /ui"
    log.info("HTTP server listening on port %d (%s)", port, endpoints)
    return server
