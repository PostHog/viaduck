"""Direct k8s Secret reads (viaduck/k8s_secrets.py) — mocked API server."""

import base64
import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from viaduck import k8s_secrets
from viaduck.k8s_secrets import SecretReadError, read_secret_key


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
