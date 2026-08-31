#!/bin/sh
# Create the dest-1 PG proxy in toxiproxy for the load-test stack.
# Idempotent: a restart of the toxiproxy container wipes its in-memory
# proxies while this init container stays exited(0) — a re-run (or a fresh
# `up` against a warm API) must tolerate the proxy already existing.
#
# nosemgrep below (trailofbits curl-unencrypted-url): the toxiproxy API is
# HTTP-only by design and lives on the throwaway compose network — no
# credentials cross it.
set -eu

until curl -fs http://toxiproxy:8474/version >/dev/null 2>&1; do sleep 1; done  # nosemgrep

curl -fs -X POST http://toxiproxy:8474/proxies -H 'Content-Type: application/json' -d '{"name":"dest1-pg","listen":"0.0.0.0:5433","upstream":"postgres-dest-1:5432","enabled":true}' || PROXY_EXISTS=1  # nosemgrep

[ "${PROXY_EXISTS:-0}" = 1 ] && curl -fs http://toxiproxy:8474/proxies/dest1-pg | grep -q postgres-dest-1:5432  # nosemgrep
