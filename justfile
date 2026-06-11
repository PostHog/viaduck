# Viaduck — DuckLake to DuckLake CDC Replication

# Default recipe: list all available recipes
default:
    @just --list

# === Dev ===

# Install dependencies
[group('dev')]
sync:
    uv sync

# Install git hooks
[group('dev')]
install-hooks:
    git config core.hooksPath .githooks

# Run the application
[group('dev')]
run *ARGS:
    uv run viaduck {{ARGS}}

# Format code
[group('dev')]
fmt:
    uv run ruff format viaduck/ tests/

# Check formatting (excludes auto-generated _version.py)
[group('dev')]
fmt-check:
    uv run ruff format --check --exclude viaduck/_version.py viaduck/ tests/

# Lint code
[group('dev')]
lint:
    uv run ruff check viaduck/ tests/

# Lint and fix
[group('dev')]
lint-fix:
    uv run ruff check --fix viaduck/ tests/

# === Test ===

# Run unit tests
[group('test')]
test:
    uv run pytest tests/unit

# Run integration tests
[group('test')]
test-integration:
    uv run pytest tests/integration

# Run performance benchmarks
[group('test')]
test-perf:
    uv run pytest tests/perf -v -s

# Run performance benchmarks and emit JSON results
[group('test')]
test-perf-json:
    uv run pytest tests/perf -v -s --perf-json perf-results.json
    @echo ""
    @echo "=== Performance Results ==="
    @python3 -c "import json; rows=json.load(open('perf-results.json')); [print(f'  {r[\"test\"]:40s} {r[\"scale\"]:30s} {r[\"elapsed_s\"]:>8.4f}s') for r in rows]"

# Verify uv.lock is consistent with pyproject.toml
[group('test')]
lock-check:
    uv lock --check

# Run semgrep security scans (mirrors Semgrep CI workflow)
[group('test')]
semgrep:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v semgrep &> /dev/null; then
        echo "ERROR: semgrep not installed. Install via: brew install semgrep"
        exit 1
    fi
    echo "=== semgrep-python ==="
    semgrep --config "p/python" --config "p/owasp-top-ten" --config "p/security-audit" --error --metrics=off --verbose viaduck/
    echo "=== semgrep-general ==="
    semgrep --config "p/owasp-top-ten" --config "p/security-audit" --config "p/trailofbits" --config "p/github-actions" --error --metrics=off --verbose --exclude ./viaduck/ .

# Full CI check (mirrors GitHub Actions CI workflow)
[group('test')]
ci: lock-check fmt-check lint test test-integration docs-check semgrep build

# === Docker ===

# Start the docker-compose dev environment
[group('docker')]
up:
    docker compose build
    docker compose up -d

# Stop the dev/demo environment (wipes volumes — next start is a fresh source)
[group('docker')]
down:
    docker compose down -v

# Open the Grafana dashboard (requires `just up` first)
[group('docker')]
dashboard:
    open http://localhost:3000/d/viaduck/viaduck

# Open the MinIO console (requires `just up` first, login: minioadmin/minioadmin)
[group('docker')]
minio:
    open http://localhost:9001

# Open the Viaduck web UI (requires `just up` first)
[group('docker')]
webui:
    open http://localhost:8000/ui

# Stand up the full stack with live producer traffic, wait for health, open the web UI
[group('docker')]
demo: up
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Waiting for viaduck to become healthy..."
    for _ in $(seq 1 60); do
        curl -fs http://localhost:8000/healthz >/dev/null 2>&1 && break
        sleep 2
    done
    if ! curl -fs http://localhost:8000/healthz >/dev/null 2>&1; then
        echo "viaduck did not become healthy — check: docker compose logs viaduck" >&2
        exit 1
    fi
    echo ""
    echo "Stack is up. The producer writes a batch every 2s; flushes land every 15s."
    echo "  Web UI:     http://localhost:8000/ui"
    echo "  Grafana:    http://localhost:3000/d/viaduck/viaduck"
    echo "  Prometheus: http://localhost:9091"
    echo "  MinIO:      http://localhost:9001  (minioadmin/minioadmin)"
    echo "Stop with: just down"
    open http://localhost:8000/ui

# === Docs ===

# Render d2 diagrams to SVG
[group('docs')]
docs:
    #!/usr/bin/env bash
    set -euo pipefail
    for f in docs/*.d2; do
        out="${f%.d2}.svg"
        d2 "$f" "$out" --theme 0
        # Normalize the build-dependent version stamp (brew: "0.7.1",
        # official tarball: "v0.7.1") so re-renders are byte-identical
        # across builds — CI's stale-diagram diff depends on it.
        sed -e 's/data-d2-version="[^"]*"/data-d2-version="0.7.1"/' "$out" > "$out.tmp" && mv "$out.tmp" "$out"
    done

# Verify all relative links in README.md point to existing files
[group('docs')]
docs-check:
    #!/usr/bin/env bash
    set -euo pipefail
    ok=true
    # Check d2 sources have corresponding SVGs
    for f in docs/*.d2; do
        svg="${f%.d2}.svg"
        if [ ! -f "$svg" ]; then
            echo "MISSING SVG: $svg (run 'just docs' to generate)"
            ok=false
        fi
    done
    # Check relative links in README.md
    grep -oE '\]\([^)]+\)' README.md \
        | sed 's/^\](\(.*\))$/\1/' \
        | grep -v '^http' \
        | grep -v '^#' \
        | while read -r link; do
            if [ ! -f "$link" ]; then
                echo "BROKEN LINK in README.md: $link"
                ok=false
            fi
        done
    if [ "$ok" = false ]; then
        exit 1
    fi
    echo "All docs links valid"

# === TLA+ ===

# Run TLA+ model checker on the viaduck CDC algorithm spec (requires flox activate)
[group('verify')]
tlc:
    cd tla && tlc Viaduck.tla -config Viaduck.cfg -workers auto

# Run TLA+ model checker with verbose output (shows state count per depth)
[group('verify')]
tlc-verbose:
    cd tla && tlc Viaduck.tla -config Viaduck.cfg -workers auto -dump dot,colorize,actionlabels states.dot

# Parse the TLA+ spec without model checking (syntax/semantic check only)
[group('verify')]
tlc-parse:
    #!/usr/bin/env bash
    set -euo pipefail
    TLA_JAR=$(sed -n 's/.*-cp \([^ ]*\).*/\1/p' "$(which tlc)")
    cd tla && java -cp "$TLA_JAR" tla2sany.SANY Viaduck.tla

# === Build ===

# Build Docker image (--no-cache avoids stale layers masking build failures)
[group('build')]
build:
    docker build --no-cache --build-arg VIADUCK_VERSION=0.0.0.dev0 -t viaduck .

# Clean build artifacts
[group('build')]
clean:
    rm -rf .venv dist *.egg-info __pycache__ viaduck/__pycache__
