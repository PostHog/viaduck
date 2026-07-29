"""Direct k8s Secret reads (viaduck/k8s_secrets.py) — mocked API server."""

import base64
import json
import time
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from viaduck import k8s_secrets, metrics
from viaduck.k8s_secrets import SecretReadError, read_secret_key


def setup_module():
    # The stale-fallback path increments a pipeline-bound counter; without
    # init the module alias is the raw labeled parent and .inc() raises —
    # INSIDE the except handler, inverting the survivable fallback into a
    # crash. Production binds in run()'s first statement; tests must too
    # (verification finding 1: this file failed in isolation, masked by
    # alphabetical suite ordering).
    metrics.init("test")


@pytest.fixture()
def _in_cluster(tmp_path, monkeypatch):
    (tmp_path / "token").write_text("sa-token\n")
    # Present but not a real PEM — the ssl context is mocked below; the
    # existence check (missing-CA fail-fast) is what matters here.
    (tmp_path / "ca.crt").write_text("dummy")
    monkeypatch.setattr(k8s_secrets, "_SA_DIR", str(tmp_path))
    monkeypatch.setattr(k8s_secrets.ssl, "create_default_context", lambda cafile=None: None)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")


def _resp(payload: dict):
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode()
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    # json.load reads via .read()
    return resp


def test_reads_and_decodes_key(_in_cluster):
    body = {"data": {"password": base64.b64encode(b"hunter2").decode()}}
    with patch("urllib.request.urlopen", return_value=_resp(body)) as uo:
        assert read_secret_key("ducklings", "cnpg-tenant-acme-password", "password") == "hunter2"
    req = uo.call_args[0][0]
    assert req.full_url == "https://10.0.0.1:443/api/v1/namespaces/ducklings/secrets/cnpg-tenant-acme-password"
    assert req.get_header("Authorization") == "Bearer sa-token"


def test_not_in_cluster_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(k8s_secrets, "_SA_DIR", str(tmp_path))  # no token file
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    with pytest.raises(SecretReadError):
        read_secret_key("ns", "name", "key")


def test_rbac_denied_names_the_fix(_in_cluster):
    err = urllib.error.HTTPError("u", 403, "Forbidden", {}, None)
    with patch("urllib.request.urlopen", side_effect=err):
        with pytest.raises(SecretReadError, match="RBAC"):
            read_secret_key("ducklings", "s", "k")


def test_missing_key_reports_count_not_names(_in_cluster):
    # Key names come from the secret payload dict — error text must not
    # carry anything derived from it (clear-text-logging taint source).
    body = {"data": {"otherkeyname": base64.b64encode(b"x").decode()}}
    with patch("urllib.request.urlopen", return_value=_resp(body)):
        with pytest.raises(SecretReadError, match="1 other key") as ei:
            read_secret_key("ns", "s", "password")
        assert "otherkeyname" not in str(ei.value)


def test_error_messages_never_contain_secret_material(_in_cluster):
    body = {"data": {"password": "!!!not-base64!!!"}}
    with patch("urllib.request.urlopen", return_value=_resp(body)):
        with pytest.raises(SecretReadError) as ei:
            read_secret_key("ns", "s", "password")
        assert "!!!" not in str(ei.value)


def test_path_traversal_names_rejected(_in_cluster):
    # namespace/name arrive from the discovery payload — a hostile value
    # must not traverse the API surface with our SA token.
    with pytest.raises(SecretReadError, match="invalid k8s"):
        read_secret_key("ducklings", "x/../../apis/rbac", "k")
    with pytest.raises(SecretReadError, match="invalid k8s"):
        read_secret_key("../kube-system", "s", "k")


def test_missing_ca_named_explicitly(tmp_path, monkeypatch):
    (tmp_path / "token").write_text("t")
    monkeypatch.setattr(k8s_secrets, "_SA_DIR", str(tmp_path))  # no ca.crt
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    with pytest.raises(SecretReadError, match="CA bundle missing"):
        read_secret_key("ns", "s", "k")


def test_namespace_label_grammar_stricter_than_name():
    # Namespaces are RFC-1123 labels: dots invalid there, valid in names.
    assert k8s_secrets._K8S_LABEL.match("ducklings")
    assert not k8s_secrets._K8S_LABEL.match("a.b")
    assert k8s_secrets._K8S_SUBDOMAIN.match("a.b")
    assert not k8s_secrets._K8S_SUBDOMAIN.match("a..b")


# ---------------------------------------------------------------------------
# read_secret_key_cached (C3 §5: TTL cache + stale-fallback)
# ---------------------------------------------------------------------------


class TestCachedReads:
    def setup_method(self):
        k8s_secrets._cache_clear()

    def teardown_method(self):
        k8s_secrets._cache_clear()

    def test_within_ttl_no_api_call(self):
        with patch("viaduck.k8s_secrets.read_secret_key", return_value="pw1") as rd:
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "pw1"
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "pw1"
        rd.assert_called_once()

    def test_expired_ttl_refreshes(self):
        with patch("viaduck.k8s_secrets.read_secret_key", return_value="pw1"):
            k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300)
        with (
            patch("viaduck.k8s_secrets.time.monotonic", return_value=time.monotonic() + 301),
            patch("viaduck.k8s_secrets.read_secret_key", return_value="pw2") as rd,
        ):
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "pw2"
        rd.assert_called_once()

    def test_stale_fallback_on_api_failure(self):
        # An API-server outage on the connect path returns the cached
        # value (the old password either still works, or the connect
        # fails into the ordinary retry) — never a fleet-wide hard fail.
        with patch("viaduck.k8s_secrets.read_secret_key", return_value="pw1"):
            k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300)
        with (
            patch("viaduck.k8s_secrets.time.monotonic", return_value=time.monotonic() + 301),
            patch(
                "viaduck.k8s_secrets.read_secret_key",
                side_effect=k8s_secrets.SecretReadError("API down"),
            ),
        ):
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "pw1"

    def test_miss_with_no_cache_raises(self):
        with (
            patch(
                "viaduck.k8s_secrets.read_secret_key",
                side_effect=k8s_secrets.SecretReadError("RBAC denied"),
            ),
            pytest.raises(k8s_secrets.SecretReadError),
        ):
            k8s_secrets.read_secret_key_cached("ns", "n", "k")

    def test_cache_keyed_per_ref(self):
        with patch("viaduck.k8s_secrets.read_secret_key", side_effect=["a", "b"]) as rd:
            assert k8s_secrets.read_secret_key_cached("ns", "n1", "k") == "a"
            assert k8s_secrets.read_secret_key_cached("ns", "n2", "k") == "b"
        assert rd.call_count == 2

    def test_stale_fallback_warn_carries_no_secret(self, caplog):
        with patch("viaduck.k8s_secrets.read_secret_key", return_value="pw1"):
            k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300)
        with (
            patch("viaduck.k8s_secrets.time.monotonic", return_value=time.monotonic() + 301),
            patch(
                "viaduck.k8s_secrets.read_secret_key",
                side_effect=k8s_secrets.SecretReadError("API down"),
            ),
            caplog.at_level("WARNING"),
        ):
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "pw1"
        assert "pw1" not in caplog.text

    def test_invalidate_forces_fresh_read(self):
        # The pool's connect-failure path: a rotated password served from
        # the warm cache heals in one flush cycle, not one TTL.
        with patch("viaduck.k8s_secrets.read_secret_key", side_effect=["old", "new"]) as rd:
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "old"
            k8s_secrets.invalidate("ns", "n", "k")
            assert k8s_secrets.read_secret_key_cached("ns", "n", "k", ttl_s=300) == "new"
        assert rd.call_count == 2
        k8s_secrets.invalidate("ns", "never", "seen")  # idempotent
