"""Direct Kubernetes Secret reads with the pod's ServiceAccount.

The dynamic-sourcing credential design (plan C4): tenant metadata-store
passwords live as Secrets in the tenant namespace (``ducklings``), and
viaduck's ServiceAccount gets a Role/RoleBinding to READ them there — no
per-tenant secret copies in the viaduck namespace, no ESO mirrors, no
plaintext in any API payload. This module is the read path: stdlib-only
(urllib + ssl with the cluster CA), no kubernetes client dependency for
one GET.

Fail-safe posture: every failure raises SecretReadError and the caller
skips that destination with a counted reason — a missing RBAC grant or a
deleted tenant secret must degrade one tenant, not the process.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import ssl
import threading
import time
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

# TTL cache for the connection-create path (C3 §5 secret-ref deferral).
# With fleets above pool_max_open, LRU churn makes secret resolution a
# routine flush-path event — an API-server outage must not become
# fleet-wide flush failures (each of which costs an evict/recreate cycle
# and ~160MB of the fork-side leak). Values are credential material:
# in-memory only, never logged.
_cache_lock = threading.Lock()
_cache: dict[tuple[str, str, str], tuple[float, str]] = {}  # (ns,name,key) -> (fetched_monotonic, value)


def _cache_clear() -> None:
    """Test hook."""
    with _cache_lock:
        _cache.clear()


_SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

# RFC-1123 subdomain grammar (what k8s enforces for namespace/Secret
# names). Validated BEFORE URL construction: these values arrive from the
# discovery payload, and an unvalidated `name` like `x/../../apis/...`
# would traverse the API surface carrying our ServiceAccount token.
# Namespaces are RFC-1123 LABELS (no dots, <=63); Secret names are
# RFC-1123 subdomains (dot-separated labels, <=253).
_K8S_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
_K8S_SUBDOMAIN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")


class SecretReadError(Exception):
    """A Secret could not be read (not in-cluster, RBAC denied, absent
    secret/key, transport failure). Message is safe to log — it never
    contains secret material."""


def _api_base() -> str:
    host = os.environ.get("KUBERNETES_SERVICE_HOST")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    if not host:
        raise SecretReadError("not running in-cluster (KUBERNETES_SERVICE_HOST unset)")
    return f"https://{host}:{port}"


def read_secret_key(namespace: str, name: str, key: str, *, timeout_s: float = 10.0) -> str:
    """Read one key from a namespaced Secret using the mounted SA token.

    The returned value is credential material: callers must never log it
    and should embed it only in in-memory connection strings.
    """
    if not _K8S_LABEL.match(namespace or ""):
        raise SecretReadError(f"invalid k8s namespace {namespace!r} (must be an RFC-1123 label)")
    if not name or len(name) > 253 or not _K8S_SUBDOMAIN.match(name):
        raise SecretReadError(f"invalid k8s secret name {name!r} (must be an RFC-1123 subdomain)")
    token_path = os.path.join(_SA_DIR, "token")
    ca_path = os.path.join(_SA_DIR, "ca.crt")
    try:
        with open(token_path) as f:
            token = f.read().strip()
    except OSError as e:
        raise SecretReadError(f"service account token unavailable: {e}") from e

    url = f"{_api_base()}/api/v1/namespaces/{namespace}/secrets/{name}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    if not os.path.exists(ca_path):
        # Falling back to system CAs would fail against the in-cluster API
        # cert with a misleading generic SSLError — name the real problem.
        raise SecretReadError(f"in-cluster CA bundle missing at {ca_path}")
    ctx = ssl.create_default_context(cafile=ca_path)
    try:
        # Scheme is pinned https by _api_base(); path components are
        # validated against the k8s name grammar above (the semgrep
        # dynamic-urllib audit rule can't see either).
        with urllib.request.urlopen(req, timeout=timeout_s, context=ctx) as resp:  # nosemgrep
            body = json.load(resp)
    except urllib.error.HTTPError as e:
        # 403 here means the RBAC grant is missing/wrong — the actionable
        # operator signal, so name it explicitly.
        detail = "RBAC denied (check the viaduck Role/RoleBinding in the tenant namespace)" if e.code == 403 else str(e)
        raise SecretReadError(f"GET secret {namespace}/{name}: HTTP {e.code} — {detail}") from e
    except Exception as e:
        raise SecretReadError(f"GET secret {namespace}/{name} failed: {e}") from e

    data = body.get("data") or {}
    if key not in data:
        # Deliberately NOT listing the available keys: they come from the
        # secret payload dict, and error text must carry nothing derived
        # from it (CodeQL's clear-text-logging taint flow ends here; the
        # operator can kubectl the Secret for its key names). The count
        # is an int — taint-free and enough to distinguish "wrong key"
        # from "empty secret".
        raise SecretReadError(f"secret {namespace}/{name} has no key {key!r} ({len(data)} other key(s) present)")
    try:
        return base64.b64decode(data[key], validate=True).decode("utf-8")
    except Exception as e:
        raise SecretReadError(f"secret {namespace}/{name} key {key!r} is not valid base64/utf-8") from e


def read_secret_key_cached(
    namespace: str,
    name: str,
    key: str,
    *,
    ttl_s: float = 300.0,
    timeout_s: float = 10.0,
) -> str:
    """read_secret_key with a TTL cache and STALE-FALLBACK: within the
    TTL the cached value is returned without an API call; past it, a
    fresh read is attempted, and on failure the stale cached value is
    returned with a WARN (safe direction — the old password either still
    works, or the connect fails and the ordinary flush-failure retry
    re-resolves after the API recovers). Only a miss-with-no-cache
    raises. Rotation heals within one TTL, or sooner via evict/recreate
    once the TTL lapses.
    """
    ck = (namespace, name, key)
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(ck)
    if entry is not None and now - entry[0] < ttl_s:
        return entry[1]
    try:
        value = read_secret_key(namespace, name, key, timeout_s=timeout_s)
    except SecretReadError as e:
        if entry is not None:
            # Scrub at the boundary (the repo convention from
            # discovery._broken): SecretReadError text is credential-free
            # by design, but scrubbing makes it a property, not a promise.
            from viaduck import metrics
            from viaduck.scrub import scrub_credentials

            metrics.secret_cache_stale_fallback_total.inc()
            log.warning(
                "Secret %s/%s read failed (%s); using cached value (age %.0fs) — "
                "a rotated password heals on the next successful read",
                namespace,
                name,
                scrub_credentials(str(e)),
                now - entry[0],
            )
            return entry[1]
        raise
    with _cache_lock:
        _cache[ck] = (now, value)
    return value


def invalidate(namespace: str, name: str, key: str) -> None:
    """Drop a cached entry. The pool calls this when a DEFERRED
    destination's connect fails: if the failure was a rotated password
    served from the warm cache, the next attempt re-reads the API and
    heals in one flush cycle instead of waiting out the TTL. A
    genuinely-down destination costs one extra API GET per attempt —
    negligible, and stale-fallback still covers API outages."""
    with _cache_lock:
        _cache.pop((namespace, name, key), None)
