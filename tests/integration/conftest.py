"""Shared integration fixtures.

The cursor store is plain Postgres (viaduck/state.py), so integration
tests get a real PG via testcontainers — one container per session, with
per-test isolation through distinct table names.
"""

from __future__ import annotations

import uuid

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_uri():
    with PostgresContainer("postgres:16-alpine") as pg:
        # testcontainers returns a sqlalchemy-flavored URL; psycopg wants plain.
        yield pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture()
def state_table_name() -> str:
    """A unique state-table name per test — cheap isolation on the shared PG."""
    return f"viaduck_state_{uuid.uuid4().hex[:12]}"
