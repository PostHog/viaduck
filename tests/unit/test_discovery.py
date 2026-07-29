"""CP-driven destination discovery (viaduck/discovery.py).

Contracts under test:
- table = the payload's events_table VERBATIM (CP owns naming)
- team `enabled` is the query-serving switch and is IGNORED (row
  presence is the only ingestion signal)
- per-entry failures skip-and-count, never raise; fetch-level failures
  raise DiscoveryError so callers keep last-known state
- static wins routing-value collisions (C6 cutover contract)
- discovered destinations carry a direct postgres URI (no env
  indirection) with convention defaults (projection on, captured_at
  dropped, memory_limit)
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from viaduck import discovery
from viaduck.k8s_secrets import SecretReadError


@pytest.fixture(autouse=True)
def _mock_metrics():
    with patch("viaduck.discovery.metrics", MagicMock()) as m:
        yield m


def _warehouse(org="acme", bucket="posthog-duckling-acme-mw-prod-us", writable=True, teams=None, ms=None):
    return {
        "org_id": org,
        "duckling_name": f"duckling-{org}",
        "state": "resharding" if not writable else "ready",
        "writable": writable,
        "bucket": bucket,
        "teams": teams
        if teams is not None
        else [
            {"team_id": 666, "schema_name": "evilco", "enabled": True, "events_table": "evilco.events"},
        ],
        "metadata_store": ms
        if ms is not None
        else {
            "kind": "cnpg-shard",
            "endpoint": "cnpg-shard-1-rw.ducklings.svc",
            "port": 5432,
            "database": "acme",
            "username": "acme_user",
            "password_secret_ref": {"namespace": "ducklings", "name": "cnpg-tenant-acme-password", "key": "password"},
        },
    }


def _payload(warehouses):
    return {"config_generation": 42, "warehouses": warehouses}


class TestMapPayload:
    def test_happy_path_events_table_verbatim(self):
        mapped = discovery.map_payload(_payload([_warehouse()]))
        assert len(mapped) == 1
        m = mapped[0]
        assert m.dest_id == "org-acme-team-666"
        assert m.team_id == 666
        # VERBATIM — viaduck never derives a name; the CP resolved it.
        assert m.table == "evilco.events"
        assert m.data_path == "s3://posthog-duckling-acme-mw-prod-us/"
        assert m.pg_endpoint == "cnpg-shard-1-rw.ducklings.svc"
        assert m.secret_namespace == "ducklings"

    def test_disabled_team_still_included(self):
        # `enabled` is the QUERY-SERVING switch (duckgres migration
        # 000024): deriving ingestion-stop from it turns a serving hold
        # into permanent event loss. Row presence is the only signal.
        wh = _warehouse(teams=[{"team_id": 7, "schema_name": "t7", "enabled": False, "events_table": "t7.events"}])
        mapped = discovery.map_payload(_payload([wh]))
        assert [m.team_id for m in mapped] == [7]

    def test_unwritable_warehouse_skipped(self, _mock_metrics):
        mapped = discovery.map_payload(_payload([_warehouse(writable=False)]))
        assert mapped == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="not_writable")

    def test_missing_bucket_skipped(self, _mock_metrics):
        mapped = discovery.map_payload(_payload([_warehouse(bucket="")]))
        assert mapped == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="no_bucket")

    def test_cnpg_row_without_connection_details_skipped(self, _mock_metrics):
        # Pre-backfill cnpg rows carry only the store KIND — must skip
        # cleanly and count, forward-compatible with the backfill landing.
        wh = _warehouse(
            ms={
                "kind": "cnpg-shard",
                "endpoint": "",
                "database": "",
                "username": "",
                "password_secret_ref": {"namespace": "", "name": "", "key": ""},
            }
        )
        mapped = discovery.map_payload(_payload([wh]))
        assert mapped == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="no_metadata_store")

    def test_missing_secret_ref_skipped(self, _mock_metrics):
        ms = _warehouse()["metadata_store"] | {"password_secret_ref": {}}
        mapped = discovery.map_payload(_payload([_warehouse(ms=ms)]))
        assert mapped == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="no_secret_ref")

    def test_bad_team_row_skips_team_not_warehouse(self):
        wh = _warehouse(
            teams=[
                {"team_id": None, "schema_name": "x", "enabled": True, "events_table": "x.events"},
                {"team_id": 8, "schema_name": "t8", "enabled": True, "events_table": "t8.events"},
            ]
        )
        mapped = discovery.map_payload(_payload([wh]))
        assert [m.team_id for m in mapped] == [8]

    def test_malformed_warehouse_degrades_not_raises(self, _mock_metrics):
        # map_payload's "never raises on data problems" contract, for
        # shapes the field-level guards don't cover.
        payload = {
            "config_generation": 1,
            "warehouses": [
                {"org_id": "bad", "writable": True, "bucket": "b", "teams": "not-a-list", "metadata_store": []},
                _warehouse(org="good"),
            ],
        }
        mapped = discovery.map_payload(payload)
        assert [m.org_id for m in mapped] == ["good"]

    def test_one_broken_warehouse_does_not_break_others(self):
        mapped = discovery.map_payload(_payload([_warehouse(org="bad", bucket=""), _warehouse(org="good")]))
        assert [m.org_id for m in mapped] == ["good"]


class TestMaterialize:
    def _mapped(self):
        return discovery.map_payload(_payload([_warehouse()]))

    def test_builds_destination_with_deferred_uri_source(self):
        # C3 §5 secret-ref deferral: materialize PROBES the secret (RBAC/
        # absence validated loudly at startup, cache warmed) but the
        # config carries the REF — the pool builds the URI per connect,
        # so rotation heals via evict/recreate. The URI's two stacked
        # parse layers are covered where the URI is now built:
        # test_attach_uri_survives_real_attach_parser +
        # TestDeferredResolution in test_destination.py.
        with patch("viaduck.discovery.read_secret_key_cached", return_value="s3cr3t") as probe:
            dests = discovery.materialize(self._mapped(), set(), {})
        assert len(dests) == 1
        d = dests[0]
        assert d.id == "org-acme-team-666"
        assert d.routing_value == "666"
        assert d.table == "evilco.events"
        probe.assert_called_once()
        assert d.postgres_uri_direct is None
        src = d.uri_source
        assert src.pg_endpoint == "cnpg-shard-1-rw.ducklings.svc"
        assert src.pg_username == "acme_user"
        assert (src.secret_namespace, src.secret_name, src.secret_key) == (
            "ducklings",
            "cnpg-tenant-acme-password",
            "password",
        )
        assert src.sslmode == "disable"
        # The property refuses: deferred configs must resolve at the pool.
        with pytest.raises(Exception, match="deferred credential resolution"):
            _ = d.postgres_uri
        assert d.schema_projection_enabled is True
        assert d.drop_source_columns == ("captured_at",)
        assert d.properties["memory_limit"] == "8GB"

    def test_defaults_overridable(self):
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(
                self._mapped(),
                set(),
                {"memory_limit": "4GB", "sslmode": "require", "drop_source_columns": []},
            )
        d = dests[0]
        assert d.properties["memory_limit"] == "4GB"
        assert d.uri_source.sslmode == "require"
        assert d.drop_source_columns == ()

    def test_static_wins_routing_collision(self):
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(self._mapped(), {"666"}, {})
        assert dests == []

    def test_password_special_characters_are_quoted(self):
        # Quoting now happens where the URI is built — per connect. The
        # direct build_attach_uri path is the contract:
        m = self._mapped()[0]
        uri = discovery.build_attach_uri(m, "p'w\\x @:/", "disable")
        # libpq quoting (backslash-escaped) then SQL doubling of every
        # single quote for the raw ATTACH '...' embedding.
        assert "password=''p\\''w\\\\x @:/''" in uri

    def test_attach_uri_survives_real_attach_parser(self):
        # THE regression test both FATALs demanded: run the produced
        # string through a real pyducklake ATTACH. An unreachable host
        # must fail at CONNECT ("Unable to connect") — a ParserException
        # means the SQL literal broke; a "Cannot open file" means the
        # postgres: prefix was lost to the FILE backend.
        import pytest as _pytest

        pyducklake = _pytest.importorskip("pyducklake")
        m = discovery.MappedDestination(
            dest_id="d",
            org_id="o",
            team_id=1,
            table="s.t",
            data_path="/tmp/viaduck-attach-test/",
            pg_endpoint="127.0.0.1",
            pg_port=1,
            pg_database="db",
            pg_username="u",
            secret_namespace="ns",
            secret_name="n",
            secret_key="k",
        )
        uri = discovery.build_attach_uri(m, "p'w \\x", "disable")
        with _pytest.raises(Exception) as ei:
            pyducklake.Catalog("attach_probe", uri, data_path="/tmp/viaduck-attach-test/")
        msg = f"{type(ei.value).__name__}: {ei.value}"
        assert "ParserException" not in msg, f"SQL literal broke: {msg[:200]}"
        assert "Cannot open file" not in msg, f"fell through to the FILE backend: {msg[:200]}"
        assert "Unable to connect" in msg or "Connection refused" in msg, msg[:200]

    def test_secret_never_in_config_at_all(self):
        # Stronger than the old repr guard: with the ref deferral the
        # credential is not IN the config object anywhere — repr, fields,
        # or otherwise.
        with patch("viaduck.discovery.read_secret_key_cached", return_value="SUPERSECRET"):
            dests = discovery.materialize(self._mapped(), set(), {})
        d = dests[0]
        assert "SUPERSECRET" not in repr(d)
        assert d.postgres_uri_direct is None
        assert "SUPERSECRET" not in repr(d.uri_source)

    def test_duplicate_routing_values_deduped_not_crashing(self, _mock_metrics):
        # A CP bug serving the same team twice must degrade the entry,
        # never reach ViaduckConfig validation and crashloop startup.
        mapped = discovery.map_payload(_payload([_warehouse(org="a1"), _warehouse(org="a2")]))
        assert len(mapped) == 2  # same team 666 under two orgs
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(mapped, set(), {})
        assert len(dests) == 1  # first wins
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="duplicate")

    def test_static_id_collision_skipped(self, _mock_metrics):
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(self._mapped(), set(), {}, static_ids={"org-acme-team-666"})
        assert dests == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="id_collision")

    def test_secret_failure_skips_and_counts(self, _mock_metrics):
        with patch("viaduck.discovery.read_secret_key_cached", side_effect=SecretReadError("RBAC denied")):
            dests = discovery.materialize(self._mapped(), set(), {})
        assert dests == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="secret_unreadable")


class TestFetch:
    def _resp(self, body: bytes):
        resp = MagicMock()
        resp.read.return_value = body
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_happy_path_and_auth_header(self):
        payload = _payload([])
        with patch.object(discovery._OPENER, "open", return_value=self._resp(json.dumps(payload).encode())) as op:
            got = discovery.fetch("http://cp/api/v1/warehouses", ("X-Duckgres-Internal-Secret", "tok"), 5.0)
        assert got["config_generation"] == 42
        req = op.call_args[0][0]
        assert req.get_header("X-duckgres-internal-secret") == "tok"

    def test_non_json_raises(self):
        with patch.object(discovery._OPENER, "open", return_value=self._resp(b"<html>")):
            with pytest.raises(discovery.DiscoveryError):
                discovery.fetch("http://cp/x", None, 5.0)

    def test_non_list_warehouses_raises(self):
        with patch.object(
            discovery._OPENER, "open", return_value=self._resp(b'{"config_generation": 1, "warehouses": null}')
        ):
            with pytest.raises(discovery.DiscoveryError):
                discovery.fetch("http://cp/x", None, 5.0)

    def test_non_numeric_generation_raises(self):
        with patch.object(
            discovery._OPENER, "open", return_value=self._resp(b'{"config_generation": "x", "warehouses": []}')
        ):
            with pytest.raises(discovery.DiscoveryError):
                discovery.fetch("http://cp/x", None, 5.0)

    def test_missing_shape_raises(self):
        with patch.object(discovery._OPENER, "open", return_value=self._resp(b'{"nope": 1}')):
            with pytest.raises(discovery.DiscoveryError):
                discovery.fetch("http://cp/x", None, 5.0)

    def test_transport_error_raises(self):
        with patch.object(discovery._OPENER, "open", side_effect=OSError("conn refused")):
            with pytest.raises(discovery.DiscoveryError):
                discovery.fetch("http://cp/x", None, 5.0)

    def test_oversize_raises(self):
        big = b"x" * (discovery._MAX_RESPONSE_BYTES + 1)
        with patch.object(discovery._OPENER, "open", return_value=self._resp(big)):
            with pytest.raises(discovery.DiscoveryError):
                discovery.fetch("http://cp/x", None, 5.0)


class TestDriftWatcher:
    def _watcher(self, baseline_payloads, gen=41):
        baseline = {m.dest_id: m for m in discovery.map_payload(_payload(baseline_payloads))}
        return discovery.DriftWatcher(
            url="http://cp/x",
            auth_header=None,
            timeout_s=5.0,
            poll_interval_s=0.01,
            baseline=baseline,
            startup_generation=gen,
        )

    def _run_once(self, w, payload_or_exc):
        kwargs = (
            {"side_effect": payload_or_exc}
            if isinstance(payload_or_exc, Exception)
            else {"return_value": payload_or_exc}
        )
        with patch("viaduck.discovery.fetch", **kwargs):
            with patch.object(w._stop, "is_set", side_effect=[False, False, True]):
                w._run()

    def test_added_removed_and_content_change_detected(self, _mock_metrics):
        w = self._watcher(
            [
                _warehouse(org="acme"),
                _warehouse(
                    org="gone", teams=[{"team_id": 1, "schema_name": "g", "enabled": True, "events_table": "g.events"}]
                ),
            ]
        )
        # acme's team stays but its metadata endpoint MOVES (post-reshard
        # split-brain tell); "gone" disappears; "fresh" appears.
        moved = _warehouse(org="acme")
        moved["metadata_store"]["endpoint"] = "cnpg-shard-9-rw.ducklings.svc"
        fresh = _warehouse(
            org="fresh", teams=[{"team_id": 9, "schema_name": "f", "enabled": True, "events_table": "f.events"}]
        )
        self._run_once(w, _payload([moved, fresh]))
        drift = {
            c.kwargs.get("kind") or c.args[0]: None
            for c in _mock_metrics.discovery_drift_destinations.labels.call_args_list
        }
        assert set(drift) == {"added", "removed", "changed"}
        set_calls = {
            (c.kwargs.get("kind") or c.args[0]): None
            for c in _mock_metrics.discovery_drift_transitions_total.labels.call_args_list
        }
        assert {"added", "removed", "changed"} <= set(set_calls)

    def test_static_wins_entries_do_not_drift(self, _mock_metrics):
        # Baseline is the MAPPED view, so an entry excluded at
        # materialization (static-wins) is in both sides — no false
        # "added forever" alert (review finding).
        w = self._watcher([_warehouse()])
        self._run_once(w, _payload([_warehouse()]))
        gauge_sets = [c for c in _mock_metrics.discovery_drift_destinations.labels.return_value.set.call_args_list]
        assert all(c.args[0] == 0 for c in gauge_sets)

    def test_unwritable_startup_org_escalates_once(self, _mock_metrics, caplog):
        import logging as _logging

        w = self._watcher([_warehouse(org="acme")])
        with caplog.at_level(_logging.ERROR, logger="viaduck.discovery"):
            self._run_once(w, _payload([_warehouse(org="acme", writable=False)]))
            self._run_once(w, _payload([_warehouse(org="acme", writable=False)]))
        assert sum("no longer writable" in r.message for r in caplog.records) == 1

    def test_watcher_survives_unexpected_exceptions(self, _mock_metrics):
        w = self._watcher([])
        self._run_once(w, TypeError("mapping blew up"))
        assert _mock_metrics.discovery_poll_failures_total.inc.called
        # synced is startup-owned: a drift-poll failure (or success) must
        # never touch it — a poll success after a failed startup would
        # otherwise clear the static-only alert (round-2 review).
        assert not _mock_metrics.discovery_synced.set.called

    def test_watcher_success_does_not_set_synced(self, _mock_metrics):
        w = self._watcher([_warehouse()])
        self._run_once(w, _payload([_warehouse()]))
        assert not _mock_metrics.discovery_synced.set.called
        assert _mock_metrics.discovery_config_generation.set.called

    def test_recovery_after_failed_startup_logs_once(self, _mock_metrics, caplog):
        import logging as _logging

        w = discovery.DriftWatcher(
            url="http://cp/x",
            auth_header=None,
            timeout_s=5.0,
            poll_interval_s=0.01,
            baseline={},
            startup_generation=-1,
        )
        with caplog.at_level(_logging.WARNING, logger="viaduck.discovery"):
            self._run_once(w, _payload([_warehouse()]))
            self._run_once(w, _payload([_warehouse()]))
        assert sum("recovered after a failed startup" in r.message for r in caplog.records) == 1

    def test_poll_failure_keeps_going(self, _mock_metrics):
        w = self._watcher([])
        self._run_once(w, discovery.DiscoveryError("down"))
        assert _mock_metrics.discovery_poll_failures_total.inc.called


class TestAllowlists:
    def _mapped(self):
        return discovery.map_payload(_payload([_warehouse()]))

    def test_endpoint_outside_suffixes_skipped(self, _mock_metrics):
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(self._mapped(), set(), {}, allowed_endpoint_suffixes=(".other.svc",))
        assert dests == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="endpoint_not_allowed")

    def test_namespace_not_allowed_skipped(self, _mock_metrics):
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(self._mapped(), set(), {}, allowed_secret_namespaces=("elsewhere",))
        assert dests == []
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="namespace_not_allowed")

    def test_defaults_pass_the_standard_convention(self):
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            dests = discovery.materialize(
                self._mapped(),
                set(),
                {},
                allowed_endpoint_suffixes=(".ducklings.svc", ".ducklings.svc.cluster.local"),
                allowed_secret_namespaces=("ducklings",),
            )
        assert len(dests) == 1


class TestMaterializeDeadline:
    def test_deadline_skips_remaining_entries(self, _mock_metrics):
        mapped = discovery.map_payload(
            _payload(
                [
                    _warehouse(org="a"),
                    _warehouse(
                        org="b",
                        teams=[{"team_id": 2, "schema_name": "t2", "enabled": True, "events_table": "t2.events"}],
                    ),
                ]
            )
        )
        clock = iter([0.0, 0.0, 100.0])  # first entry inside, second past deadline

        def slow_read(*a, **k):
            return "pw"

        with patch("viaduck.discovery.read_secret_key_cached", side_effect=slow_read):
            with patch("viaduck.discovery.time.monotonic", side_effect=lambda: next(clock, 100.0)):
                dests = discovery.materialize(mapped, set(), {}, deadline_s=10.0)
        assert len(dests) == 1
        _mock_metrics.discovery_broken_entries_total.labels.assert_called_with(reason="deadline")

    def test_heartbeat_called_per_entry(self):
        beats = []
        with patch("viaduck.discovery.read_secret_key_cached", return_value="pw"):
            discovery.materialize(
                discovery.map_payload(_payload([_warehouse()])), set(), {}, heartbeat=lambda: beats.append(1)
            )
        assert beats == [1]
