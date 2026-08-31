"""Load generator: high-volume, realistically-shaped traffic against the
source DuckLake catalog. Replaces producer.py in the load profile.

Shape goals (mirroring what prod taught us the hard way):
  - Volume: head tenant outruns the flush interval so its flushes are
    TARGET-triggered, not interval-triggered (default ~3K rows/s aggregate,
    Zipf-skewed across tenants — head ~35% of routed (weights 1/r^1.2 over
    21 companies: 1/2.885), tail trickles).
  - Width: rows carry a wide, LOW-CARDINALITY string payload — the
    dictionary-sharing shape that made pa.Table.nbytes lie ~100x on the
    read side (2026-08-26). The load test exists so the controller's
    behavior is observable without deploying to prod; row shape is part
    of that fidelity.
  - Skew: 20 routed companies + tealford (unrouted) with power-law
    weights, same as producer.py, so the tail stays interval-bound.

Env:
  LOAD_ROWS_PER_SECOND   aggregate insert rate (default 3000)
  LOAD_ROW_WIDTH_BYTES   approx bytes per row, dominated by payload (default 2048)
  LOAD_BLOB_CARDINALITY  distinct payload values (default 64 — low-card)
  LOAD_BURST_COMPANY     tail tenant that goes hot mid-run (default brantley;
                         empty disables) — the distribution-shift scenario
  LOAD_BURST_START_S     seconds after start when the burst begins (default 190)
  LOAD_BURST_ROWS_PER_SECOND  burst rate for that tenant (default 1500)

The burst stream answers "does a thin→hot tenant migrate interval→target
with no lag excursion" — the failure mode class that byte-denominated
sizing kept re-opening, exercised without a prod deploy. The burst tenant
(brantley) is on the UNCONTENDED catalog, isolating the shift signal from
the latency wave.
"""

import os
import random
import signal
import string
import sys
import time

import pyarrow as pa
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, StringType

POSTGRES_URI = os.environ["SOURCE_POSTGRES_URI"]
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "minio:9000")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "minioadmin")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "false")
S3_URL_STYLE = os.environ.get("S3_URL_STYLE", "path")
S3_DATA_PATH = os.environ.get("S3_DATA_PATH", "s3://source/")

RATE = float(os.environ.get("LOAD_ROWS_PER_SECOND", "3000"))
ROW_WIDTH = int(os.environ.get("LOAD_ROW_WIDTH_BYTES", "2048"))
BLOB_CARDINALITY = int(os.environ.get("LOAD_BLOB_CARDINALITY", "64"))
BURST_COMPANY = os.environ.get("LOAD_BURST_COMPANY", "brantley")
BURST_START_S = float(os.environ.get("LOAD_BURST_START_S", "190"))
BURST_RATE = float(os.environ.get("LOAD_BURST_ROWS_PER_SECOND", "1500"))
TICK_SECONDS = 0.25

COMPANIES = [
    "quacksworth",
    "mallardine",
    "drakeford",
    "gosswick",
    "eiderdown",
    "pintailor",
    "scaupson",
    "wigeonton",
    "gadwallace",
    "smewbury",
    "pochardly",
    "canvasbeck",
    "redheadly",
    "buffleton",
    "goldeneye",
    "merganser",
    "ruddyshore",
    "shovelnook",
    "woodington",
    "brantley",
    "tealford",
]
_POWER_LAW_S = 1.2
COMPANY_WEIGHTS = [1.0 / (rank**_POWER_LAW_S) for rank in range(1, len(COMPANIES) + 1)]

shutdown = False
next_event_id = 1


def _signal_handler(signum, frame):
    global shutdown
    print(f"Received signal {signum}, shutting down gracefully...")
    shutdown = True


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)

# Low-cardinality blob pool: ~64 distinct wide strings. Drawn per row, they
# dictionary-encode to nearly nothing on the wire while inflating naive
# per-row byte accounting — the prod shape.
PAYLOAD_WIDTH = max(16, ROW_WIDTH - 40)  # company + ids + slack
BLOB_POOL = [
    "".join(random.choices(string.ascii_letters + string.digits, k=PAYLOAD_WIDTH)) for _ in range(BLOB_CARDINALITY)
]


def connect_catalog() -> Catalog:
    properties = {
        "s3_endpoint": S3_ENDPOINT,
        "s3_access_key_id": S3_ACCESS_KEY_ID,
        "s3_secret_access_key": S3_SECRET_ACCESS_KEY,
        "s3_use_ssl": S3_USE_SSL,
        "s3_url_style": S3_URL_STYLE,
    }
    return Catalog("source", POSTGRES_URI, data_path=S3_DATA_PATH, properties=properties)


def ensure_table(catalog: Catalog):
    try:
        return catalog.load_table("events")
    except Exception:
        schema = Schema.of(
            {
                "event_id": IntegerType(),
                "company": StringType(),
                "payload": StringType(),
                "value": IntegerType(),
            }
        )
        table = catalog.create_table("events", schema)
        print("Created table: events (load schema: + payload VARCHAR)")
        return table


def generate_batch(count: int) -> pa.Table:
    global next_event_id
    event_ids = list(range(next_event_id, next_event_id + count))
    next_event_id += count
    companies = random.choices(COMPANIES, weights=COMPANY_WEIGHTS, k=count)
    payloads = random.choices(BLOB_POOL, k=count)
    values = [random.randint(1, 1000) for _ in range(count)]
    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "company": pa.array(companies, type=pa.string()),
            "payload": pa.array(payloads, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def generate_burst_batch(count: int) -> pa.Table:
    """Burst rows for the one tenant going hot — same shape, fixed company."""
    global next_event_id
    event_ids = list(range(next_event_id, next_event_id + count))
    next_event_id += count
    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "company": pa.array([BURST_COMPANY] * count, type=pa.string()),
            "payload": pa.array(random.choices(BLOB_POOL, k=count), type=pa.string()),
            "value": pa.array([random.randint(1, 1000) for _ in range(count)], type=pa.int32()),
        }
    )


def main():
    print("Connecting to source DuckLake catalog...")
    catalog = connect_catalog()
    table = ensure_table(catalog)

    total = 0
    burst_total = 0
    tick = 0
    start = time.monotonic()
    burst_start_at = start + BURST_START_S if BURST_COMPANY else None
    print(f"Starting loadgen: rate={RATE} rows/s, row_width~{ROW_WIDTH}B, blob_cardinality={BLOB_CARDINALITY}")
    if burst_start_at is not None:
        print(f"Burst scheduled: {BURST_COMPANY} goes to {BURST_RATE} rows/s at t+{BURST_START_S}s")

    while not shutdown:
        # Wall-clock schedule, not sleep-until: a slow append (>TICK_SECONDS)
        # must produce a catch-up batch next tick, not a permanently sagged
        # rate — the checker's batch-shape band assumes the rate is real.
        tick += 1
        due = int(RATE * (time.monotonic() - start)) - total
        if due > 0:
            table.append(generate_batch(due))
            total += due
        if burst_start_at is not None and time.monotonic() >= burst_start_at:
            if burst_total == 0:
                print(f"=== BURST ON: {BURST_COMPANY} at {BURST_RATE} rows/s ===", flush=True)
            burst_due = int(BURST_RATE * (time.monotonic() - burst_start_at)) - burst_total
            if burst_due > 0:
                table.append(generate_burst_batch(burst_due))
                burst_total += burst_due
        if tick % 80 == 0:  # every ~20s
            print(
                f"  tick={tick} total_inserted={total} burst={burst_total} "
                f"(~{total / max(1.0, time.monotonic() - start):.0f} rows/s base)"
            )
        delay = start + tick * TICK_SECONDS - time.monotonic()
        if not shutdown and delay > 0:
            time.sleep(delay)

    print(f"Shutdown. Total inserted: {total}")


if __name__ == "__main__":
    sys.exit(main())
