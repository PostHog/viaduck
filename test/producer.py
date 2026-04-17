"""Standalone producer that writes events to a source DuckLake catalog.

Generates insert/update/delete traffic against an `events` table with schema:
  event_id INT, company VARCHAR, value INT

Companies are randomly chosen from: quacksworth, mallardine, tealford.
tealford is intentionally unrouted in the dev config to exercise unrouted metrics.
"""

import os
import random
import signal
import sys
import time

import pyarrow as pa
from pyducklake import Catalog, Schema
from pyducklake.types import IntegerType, StringType

# --- Configuration from environment ---

POSTGRES_URI = os.environ["SOURCE_POSTGRES_URI"]
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "minio:9000")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "minioadmin")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "false")
S3_URL_STYLE = os.environ.get("S3_URL_STYLE", "path")
S3_DATA_PATH = os.environ.get("S3_DATA_PATH", "s3://source/")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))
BATCH_INTERVAL = float(os.environ.get("BATCH_INTERVAL", "2"))

COMPANIES = ["quacksworth", "mallardine", "tealford"]

# --- Globals ---

shutdown = False
next_event_id = 1
# Track inserted event_ids by company for updates/deletes
live_rows: dict[int, str] = {}  # event_id -> company


def _signal_handler(signum, frame):
    global shutdown
    print(f"Received signal {signum}, shutting down gracefully...")
    shutdown = True


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


def connect_catalog() -> Catalog:
    """Connect to the source DuckLake catalog."""
    properties = {
        "s3_endpoint": S3_ENDPOINT,
        "s3_access_key_id": S3_ACCESS_KEY_ID,
        "s3_secret_access_key": S3_SECRET_ACCESS_KEY,
        "s3_use_ssl": S3_USE_SSL,
        "s3_url_style": S3_URL_STYLE,
    }
    return Catalog("source", POSTGRES_URI, data_path=S3_DATA_PATH, properties=properties)


def ensure_table(catalog: Catalog):
    """Create the events table if it doesn't exist."""
    try:
        return catalog.load_table("events")
    except Exception:
        schema = Schema.of(
            {
                "event_id": IntegerType(),
                "company": StringType(),
                "value": IntegerType(),
            }
        )
        table = catalog.create_table("events", schema)
        print("Created table: events")
        return table


def generate_inserts(count: int) -> pa.Table:
    """Generate a batch of new insert rows."""
    global next_event_id

    event_ids = []
    companies = []
    values = []

    for _ in range(count):
        eid = next_event_id
        next_event_id += 1
        company = random.choice(COMPANIES)
        value = random.randint(1, 1000)

        event_ids.append(eid)
        companies.append(company)
        values.append(value)
        live_rows[eid] = company

    return pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "company": pa.array(companies, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


def do_updates(table, count: int):
    """Update random existing rows (change value, NOT company)."""
    if not live_rows:
        return 0

    count = min(count, len(live_rows))
    eids_to_update = random.sample(list(live_rows.keys()), count)

    event_ids = []
    companies = []
    values = []

    for eid in eids_to_update:
        event_ids.append(eid)
        companies.append(live_rows[eid])  # keep same company
        values.append(random.randint(1, 1000))

    update_batch = pa.table(
        {
            "event_id": pa.array(event_ids, type=pa.int32()),
            "company": pa.array(companies, type=pa.string()),
            "value": pa.array(values, type=pa.int32()),
        }
    )

    table.upsert(update_batch, join_cols=["event_id"])
    return count


def do_deletes(table, count: int):
    """Delete random existing rows."""
    if not live_rows:
        return 0

    count = min(count, len(live_rows))
    eids_to_delete = random.sample(list(live_rows.keys()), count)

    for eid in eids_to_delete:
        table.delete(f"event_id = {eid}")
        del live_rows[eid]

    return count


def main():
    print("Connecting to source DuckLake catalog...")
    catalog = connect_catalog()
    table = ensure_table(catalog)

    cycle = 0
    total_inserted = 0
    total_updated = 0
    total_deleted = 0

    print(f"Starting producer loop: batch_size={BATCH_SIZE}, interval={BATCH_INTERVAL}s")

    while not shutdown:
        cycle += 1

        # Always insert
        insert_batch = generate_inserts(BATCH_SIZE)
        table.append(insert_batch)
        total_inserted += BATCH_SIZE

        # Occasionally update (~30% of cycles, after we have some rows)
        updated = 0
        if len(live_rows) > 10 and random.random() < 0.3:
            update_count = random.randint(1, min(3, len(live_rows)))
            updated = do_updates(table, update_count)
            total_updated += updated

        # Occasionally delete (~15% of cycles, after we have some rows)
        deleted = 0
        if len(live_rows) > 20 and random.random() < 0.15:
            delete_count = random.randint(1, min(2, len(live_rows)))
            deleted = do_deletes(table, delete_count)
            total_deleted += deleted

        if cycle % 5 == 0:
            print(
                f"  cycle={cycle} inserted={total_inserted} updated={total_updated} "
                f"deleted={total_deleted} live={len(live_rows)}"
            )

        if not shutdown:
            time.sleep(BATCH_INTERVAL)

    print(
        f"Shutdown. Total: inserted={total_inserted} updated={total_updated} "
        f"deleted={total_deleted} live={len(live_rows)}"
    )


if __name__ == "__main__":
    sys.exit(main())
