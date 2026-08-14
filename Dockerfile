# 0.7 -> 0.9: relative exclude-newer ("7 days") needs modern uv;
# keep in lockstep with the setup-uv pin in .github/workflows/.
FROM ghcr.io/astral-sh/uv:0.12 AS uv
FROM python:3.12-slim AS builder

COPY --from=uv /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock LICENSE ./
RUN uv sync --frozen --no-dev --no-install-project

COPY viaduck/ viaduck/
ARG VIADUCK_VERSION=0.0.0.dev0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=$VIADUCK_VERSION
RUN uv sync --frozen --no-dev

# Overwrite the PyPI 1.5.5 wheel with the PostHog fork (VARIANT shred
# allowlist + extract pushdown). version stays 1.5.5 so INSTALL ducklake
# still hits the official 1.5.5 channel (SHA asserted below).
ARG TARGETARCH
ARG DUCKDB_RELEASE=v1.5.5-posthog.2
RUN test -n "$TARGETARCH" \
    && case "$TARGETARCH" in amd64) WHEEL_ARCH=x86_64 ;; arm64) WHEEL_ARCH=aarch64 ;; *) echo "unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; esac \
    && apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL "https://github.com/PostHog/duckdb/releases/download/${DUCKDB_RELEASE}/duckdb-1.5.5-cp312-cp312-linux_${WHEEL_ARCH}.whl" -o /tmp/duckdb.whl \
    && uv pip install --python /app/.venv/bin/python --no-deps /tmp/duckdb.whl \
    && rm /tmp/duckdb.whl \
    && apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

FROM python:3.12-slim

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/viaduck /app/viaduck

ENV PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home --shell /bin/false viaduck
USER viaduck

# Pre-install DuckDB extensions at build time to avoid runtime network dependency.
# httpfs must be installed before ducklake — there's a race condition with S3 access
# if ducklake loads first and tries to use httpfs before it's available.
#
# The ducklake build is ASSERTED: INSTALL is unpinned by design (the channel
# serves one build per duckdb version), which let the 2026-04-09 build
# 415a9ebd — whose per-connection stats/schemas caches grow unbounded under
# advancing snapshots (the 4-6h OOM cycle; see hypothesis-1.md) — ship
# silently for months. duckdb 1.5.5's channel serves d8a1881e (2026-07-20,
# post-fix). If a duckdb bump changes the served build, this assertion makes
# it a VISIBLE build failure: verify the new build against the leak repro in
# hypothesis-1.md, then update the hash here.
RUN python -c "\
import duckdb; c = duckdb.connect(); \
ver, sid = c.execute('SELECT library_version, source_id FROM pragma_version()').fetchone(); \
assert ver == 'v1.5.5', ver; \
assert sid.startswith('2a514c18f7'), f'expected PostHog fork source_id, got {sid!r}'; \
c.execute('INSTALL httpfs'); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres'); \
v = c.execute(\"SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'\").fetchone()[0]; \
assert v == 'd8a1881e', f'unexpected ducklake build {v!r} — see Dockerfile comment'"

HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz')"]

ENTRYPOINT ["viaduck"]
