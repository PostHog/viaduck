"""Behavioral checker for the load-test stack (docker-compose.load.yaml).

Polls viaduck's /metrics through a baseline → contention → recovery
schedule and asserts the flush-sizing controller does the right things
WITHOUT a prod deploy. "Contention" is a catalog-latency wave that this
script injects and removes itself via the toxiproxy API on the dest-1
Postgres proxy (see docker-compose.load.yaml for why latency, not
competing commits) — phase boundaries are exact, no schedule to match.

  HARD
  1. head tenant flushes on the target trigger (bytes today, rows
     post-change) during baseline — not the interval;
  2. CRUMB DETECTOR: head's mean rows/flush during baseline sits in an
     ABSOLUTE band (default [8K, 70K] — the load shape is deterministic:
     ~2KB rows, so the 32MiB byte target prices ~16K-row flushes and the
     60K rows ceiling caps at 60K). Self-relative assertions cannot see
     crumb-cutting (the 08-26 / 08-28-redux incidents): the controller is
     internally consistent at crumb sizes, the batch shape is not;
  3. under catalog latency, >=1 contended destination's flush target
     HALVES from its baseline (the AIMD learned down);
  4. control destinations (uncontended catalog) do NOT move their target
     (the differential half — a global cause would drop everyone);
  5. zero errors over the whole run;
  6. recovery: buffers return to the baseline steady-state band and head
     lag stays under 2x flush interval;
  7. thin tenants stay interval-bound (>=5 destinations with interval
     flushes) — the 08-27 commit-rate reduction is retained;
  8. DISTRIBUTION SHIFT: the burst tenant (thin→hot mid-run, loadgen's
     LOAD_BURST_* stream) migrates its trigger mix — interval-bound
     BEFORE the burst, target-triggered AFTER — with no config change;
  9. the burst tenant's lag stays <= 2x flush interval THROUGH the shift
     (a burst that wedges its destination is the regression this harness
     exists to catch pre-deploy).
  SOFT (warn only): sliced trigger observed for the head (backlog slicing
  exercised).

--burst-dest / --burst-start must match the loadgen's LOAD_BURST_COMPANY /
LOAD_BURST_START_S (defaults are paired). --burst-dest="" disables 8-9.

Works against both the byte controller (viaduck_dest_flush_target_bytes)
and the rows controller (viaduck_dest_flush_target_rows) so before/after
runs of the sizing PR are directly comparable.

Usage: uv run python test/loadcheck.py [--warmup 60 --on 90 --off 150]
"""

import argparse
import json
import re
import sys
import time
import urllib.request

CONTENDED = [
    "quacksworth-lake",
    "drakeford-lake",
    "gosswick-lake",
    "eiderdown-lake",
    "pintailor-lake",
    "scaupson-lake",
    "wigeonton-lake",
    "gadwallace-lake",
    "smewbury-lake",
    "pochardly-lake",
    "canvasbeck-lake",
    "redheadly-lake",
    "buffleton-lake",
    "goldeneye-lake",
]
# Control includes mallardine, a target-bound mid-head tenant: the Zipf
# tail is interval-bound at 120s and would otherwise give the differential
# assertion nothing to evaluate (no baseline flush → cold-start skip).
CONTROL = [
    "mallardine-lake",
    "merganser-lake",
    "ruddyshore-lake",
    "shovelnook-lake",
    "woodington-lake",
    "brantley-lake",
]

_SERIES = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)\s*$")
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')

# Size-trigger label across generations: the rows controller ("target"),
# the byte controller ("bytes"), and the legacy fixed rows cap ("rows",
# deleted by the rows PR but accepted so interim builds stay checkable).
TARGET_TRIGGERS = ("target", "bytes", "rows")

TOXIC_NAME = "dest1-catalog-latency"


def set_latency(api: str, proxy: str, latency_ms: int, jitter_ms: int, enable: bool) -> None:
    if enable:
        body = json.dumps(
            {
                "name": TOXIC_NAME,
                "type": "latency",
                "toxicity": 1.0,
                "attributes": {"latency": latency_ms, "jitter": jitter_ms},
            }
        ).encode()
        req = urllib.request.Request(
            f"{api}/proxies/{proxy}/toxics", data=body, headers={"Content-Type": "application/json"}
        )
        try:
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            raise SystemExit(f"loadcheck: failed to add latency toxic on proxy {proxy!r}: {e}") from e
    else:
        req = urllib.request.Request(f"{api}/proxies/{proxy}/toxics/{TOXIC_NAME}", method="DELETE")
        try:
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass  # already gone (or never added) — the goal state either way


def fetch_metrics(url: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    out: dict[str, list[tuple[dict[str, str], float]]] = {}
    with urllib.request.urlopen(url + "/metrics", timeout=10) as resp:
        text = resp.read().decode()
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _SERIES.match(line)
        if not m:
            continue
        name, labels_raw, value = m.groups()
        labels = dict(_LABEL.findall(labels_raw or ""))
        try:
            out.setdefault(name, []).append((labels, float(value)))
        except ValueError:
            continue
    return out


def gauge_by_dest(series: list[tuple[dict[str, str], float]]) -> dict[str, float]:
    return {labels["destination"]: v for labels, v in series if "destination" in labels}


class Snapshot:
    def __init__(self, metrics: dict[str, list[tuple[dict[str, str], float]]]):
        # Dual-gauge: rows controller preferred, byte controller fallback.
        target = metrics.get("viaduck_dest_flush_target_rows") or metrics.get("viaduck_dest_flush_target_bytes") or []
        self.target_unit = "rows" if metrics.get("viaduck_dest_flush_target_rows") else "bytes"
        self.targets = gauge_by_dest(target)
        self.flushes: dict[tuple[str, str], float] = {}
        for labels, v in metrics.get("viaduck_delivery_flushes_total", []):
            self.flushes[(labels.get("destination", "?"), labels.get("trigger", "?"))] = v
        self.dur_count = gauge_by_dest(metrics.get("viaduck_delivery_flush_seconds_count", []))
        self.dur_sum = gauge_by_dest(metrics.get("viaduck_delivery_flush_seconds_sum", []))
        self.rows_written = gauge_by_dest(metrics.get("viaduck_dest_rows_written_total", []))
        self.buffer_rows = gauge_by_dest(metrics.get("viaduck_delivery_buffer_rows", []))
        self.lag = gauge_by_dest(metrics.get("viaduck_dest_time_lag_seconds", []))
        self.errors = sum(v for _, v in metrics.get("viaduck_errors_total", []))


def fmt_target(v: float, unit: str) -> str:
    if unit == "rows":
        return f"{v / 1000:.1f}K rows"
    return f"{v / 1048576:.1f}MiB"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument(
        "--warmup",
        type=float,
        default=90,
        help="baseline seconds (latency-free). Must exceed the head's first-flush cadence under BOTH "
        "controller generations: ~16s (32MiB byte target) and ~58s (60K rows ceiling at ~1K rows/s) "
        "— 60s is knife-edge against the rows generation",
    )
    ap.add_argument("--on", type=float, default=90, help="catalog-latency wave seconds")
    ap.add_argument("--off", type=float, default=150, help="recovery seconds")
    ap.add_argument("--poll", type=float, default=10)
    ap.add_argument("--head", default="quacksworth-lake")
    ap.add_argument("--flush-interval", type=float, default=120)
    ap.add_argument("--toxiproxy-api", default="http://localhost:8474")
    ap.add_argument("--proxy", default="dest1-pg")
    ap.add_argument("--latency-ms", type=int, default=300)
    ap.add_argument("--jitter-ms", type=int, default=50)
    ap.add_argument(
        "--head-min-rows-per-flush",
        type=float,
        default=8000,
        help="crumb floor: below this the head is being crumb-cut (want ~16K at 32MiB x ~2KB rows)",
    )
    ap.add_argument(
        "--head-max-rows-per-flush",
        type=float,
        default=70000,
        help="oversize ceiling: above this the head is batching past the 60K rows cap",
    )
    ap.add_argument(
        "--burst-dest",
        default="brantley-lake",
        help="destination id of loadgen's burst tenant (empty string disables the distribution-shift assertions)",
    )
    ap.add_argument(
        "--burst-start",
        type=float,
        default=190,
        help="seconds into the run when the burst begins — must match loadgen's LOAD_BURST_START_S",
    )
    args = ap.parse_args()

    # Pre-flight: the latency lever must exist before we spend minutes of
    # baseline. A missing proxy means the stack came up without the load
    # overlay — say so, don't traceback at t=warmup.
    try:
        with urllib.request.urlopen(f"{args.toxiproxy_api}/proxies/{args.proxy}", timeout=5) as r:
            json.load(r)
    except Exception as e:
        print(
            f"FAIL: toxiproxy proxy {args.proxy!r} not reachable at {args.toxiproxy_api} ({e})\n"
            "Is the load stack up WITH the load overlay? (`just load-up`, not `just up`)"
        )
        return 2

    total = args.warmup + args.on + args.off
    print(
        f"loadcheck: warmup={args.warmup}s on={args.on}s off={args.off}s poll={args.poll}s head={args.head} "
        f"latency={args.latency_ms}ms"
    )

    snaps: list[tuple[float, str, Snapshot]] = []
    t0 = time.monotonic()
    peak_buffer = 0.0
    latency_on = False
    prev_phase = "baseline"
    try:
        while True:
            elapsed = time.monotonic() - t0
            if elapsed > total:
                break
            phase = (
                "baseline" if elapsed < args.warmup else ("contend" if elapsed < args.warmup + args.on else "recover")
            )
            if phase != prev_phase:
                if phase == "contend":
                    set_latency(args.toxiproxy_api, args.proxy, args.latency_ms, args.jitter_ms, True)
                    latency_on = True
                    print(
                        f"--- t={elapsed:.0f}s: latency wave ON ({args.latency_ms}ms on {args.proxy}) ---", flush=True
                    )
                elif phase == "recover":
                    set_latency(args.toxiproxy_api, args.proxy, 0, 0, False)
                    latency_on = False
                    print(f"--- t={elapsed:.0f}s: latency wave OFF ---", flush=True)
                prev_phase = phase
            try:
                snap = Snapshot(fetch_metrics(args.url))
            except Exception as e:
                print(f"t={elapsed:6.0f}s [{phase:8s}] metrics fetch failed: {e}")
                time.sleep(args.poll)
                continue
            # Head mean flush duration since the previous poll — the
            # controller's feedback signal, watch it cross the band.
            prev_dur_count, prev_dur_sum = (
                (snaps[-1][2].dur_count.get(args.head, 0.0), snaps[-1][2].dur_sum.get(args.head, 0.0))
                if snaps
                else (0.0, 0.0)
            )
            snaps.append((elapsed, phase, snap))
            total_buf = sum(snap.buffer_rows.values())
            peak_buffer = max(peak_buffer, total_buf)
            head_tgt = snap.targets.get(args.head, 0.0)
            contended_min = min((snap.targets.get(d, float("inf")) for d in CONTENDED), default=0.0)
            head_flushes = sum(v for (d, _), v in snap.flushes.items() if d == args.head)
            d_count = snap.dur_count.get(args.head, 0.0) - prev_dur_count
            head_dur = (
                f"{(snap.dur_sum.get(args.head, 0.0) - prev_dur_sum) / d_count:5.1f}s" if d_count > 0 else "    - "
            )
            print(
                f"t={elapsed:6.0f}s [{phase:8s}] errs={snap.errors:.0f} buf={total_buf / 1000:7.0f}K rows "
                f"lag(head)={snap.lag.get(args.head, 0.0):6.1f}s head: tgt={fmt_target(head_tgt, snap.target_unit)} "
                f"flushes={head_flushes:.0f} dur={head_dur} | "
                f"contended-min tgt={fmt_target(contended_min, snap.target_unit)}",
                flush=True,
            )
            time.sleep(args.poll)
    finally:
        if latency_on:
            set_latency(args.toxiproxy_api, args.proxy, 0, 0, False)
            print("--- latency toxic removed (cleanup) ---")

    if not snaps:
        print("FAIL: never fetched metrics once — is the load stack up? (`just load-up`)")
        return 1

    unit = snaps[-1][2].target_unit
    baseline = [s for _, p, s in snaps if p == "baseline"]
    contend = [s for _, p, s in snaps if p == "contend"]
    last = snaps[-1][2]
    first = snaps[0][2]

    def flushes_during(snaps_in, dest, triggers):
        if not snaps_in:
            return 0.0
        return sum(snaps_in[-1].flushes.get((dest, t), 0.0) - snaps_in[0].flushes.get((dest, t), 0.0) for t in triggers)

    results: list[tuple[bool, str]] = []

    # 1. head flushes on the target trigger during baseline
    head_target_flushes = flushes_during(baseline, args.head, TARGET_TRIGGERS)
    results.append(
        (head_target_flushes > 0, f"head target-triggered flushes in baseline: {head_target_flushes:.0f} (want > 0)")
    )

    # 2. crumb detector: absolute batch-shape band on the head's BASELINE
    # flushes (contention-phase batches legitimately shrink with the target)
    if len(baseline) >= 2:
        rows_delta = baseline[-1].rows_written.get(args.head, 0.0) - baseline[0].rows_written.get(args.head, 0.0)
        flush_delta = sum(
            baseline[-1].flushes.get((args.head, t), 0.0) - baseline[0].flushes.get((args.head, t), 0.0)
            for t in {trig for (_, trig) in baseline[-1].flushes} | {trig for (_, trig) in baseline[0].flushes}
        )
        mean_rows = rows_delta / flush_delta if flush_delta > 0 else 0.0
        results.append(
            (
                flush_delta > 0 and args.head_min_rows_per_flush <= mean_rows <= args.head_max_rows_per_flush,
                f"head baseline rows/flush: {mean_rows:.0f} over {flush_delta:.0f} flushes "
                f"(want [{args.head_min_rows_per_flush:.0f}, {args.head_max_rows_per_flush:.0f}])",
            )
        )
    else:
        results.append((False, "head baseline rows/flush: <2 baseline snapshots sampled (run longer warmup)"))

    # 3. contention halves at least one contended destination's target
    halved = []
    for d in CONTENDED:
        base_max = max((s.targets.get(d, 0.0) for s in baseline), default=0.0)
        cont_min = min((s.targets.get(d, float("inf")) for s in contend), default=float("inf"))
        if base_max > 0 and cont_min <= 0.5 * base_max:
            halved.append(d)
    results.append(
        (
            bool(halved),
            f"contended destinations whose target halved under contention: {len(halved)} {halved[:3]} (want >= 1)",
        )
    )

    # 4. control destinations' targets do NOT move. Compared against the
    # baseline MIN (not max): the gauge is seeded at the ceiling before any
    # flush, so a control destination whose cold-start first flush is slow
    # enough to halve reads 32MiB-base even when it sat flat all baseline —
    # baseline_min absorbs that. And destinations with NO baseline flush are
    # skipped entirely (no steady-state baseline exists to compare against).
    moved_control = []
    skipped_cold = []
    for d in CONTROL:
        base_flush_count = (
            sum(
                baseline[-1].flushes.get((d, t), 0.0) - baseline[0].flushes.get((d, t), 0.0)
                for t in {trig for (_, trig) in baseline[-1].flushes} | {trig for (_, trig) in baseline[0].flushes}
            )
            if len(baseline) >= 2
            else 0.0
        )
        if base_flush_count == 0:
            skipped_cold.append(d)
            continue
        base_min = min((s.targets.get(d, float("inf")) for s in baseline), default=float("inf"))
        cont_min = min((s.targets.get(d, float("inf")) for s in contend), default=float("inf"))
        if base_min != float("inf") and cont_min < 0.9 * base_min:
            moved_control.append(d)
    results.append(
        (
            not moved_control,
            f"control destinations whose target moved during contention: {moved_control or 'none'} (want none)"
            + (f"; skipped cold-start: {skipped_cold}" if skipped_cold else ""),
        )
    )

    # 5. zero errors
    results.append((last.errors - first.errors == 0, f"errors over run: {last.errors - first.errors:.0f} (want 0)"))

    # 6. recovery: buffers back to the baseline steady-state band (NOT zero —
    # interval-bound tenants legitimately accumulate ~one interval of rows),
    # head lag bounded
    baseline_buf = [sum(s.buffer_rows.values()) for s in baseline] or [0.0]
    final_bufs = [sum(s.buffer_rows.values()) for _, _, s in snaps[-3:]]
    baseline_mean = sum(baseline_buf) / len(baseline_buf)
    final_mean = sum(final_bufs) / len(final_bufs)
    results.append(
        (
            final_mean <= max(5000, 1.5 * baseline_mean),
            f"buffer rows at end: {final_mean:.0f} (baseline mean {baseline_mean:.0f}, peak {peak_buffer:.0f}; "
            "want <= 1.5x baseline)",
        )
    )
    head_lag = last.lag.get(args.head, 0.0)
    results.append(
        (
            head_lag <= 2 * args.flush_interval,
            f"head lag at end: {head_lag:.0f}s (want <= {2 * args.flush_interval:.0f}s)",
        )
    )

    # 7. thin tenants interval-bound
    all_snaps = [s for _, _, s in snaps]
    interval_tenants = sum(1 for d in CONTENDED + CONTROL if flushes_during(all_snaps, d, ("interval",)) > 0)
    results.append((interval_tenants >= 5, f"destinations with interval flushes: {interval_tenants} (want >= 5)"))

    # 8+9. distribution shift: the burst tenant migrates interval → target
    # with no lag excursion. The burst stream lives on the CONTROL catalog,
    # isolated from the latency wave.
    if args.burst_dest:
        pre_burst = [s for e, _, s in snaps if e < args.burst_start]
        post_burst = [s for e, _, s in snaps if e >= args.burst_start]
        if len(pre_burst) < 2 or len(post_burst) < 2:
            results.append(
                (
                    False,
                    f"burst: need >=2 metric snapshots on both sides of t={args.burst_start}s "
                    f"(got {len(pre_burst)}/{len(post_burst)}) — check --burst-start vs the schedule",
                )
            )
        else:
            pre_interval = flushes_during(pre_burst, args.burst_dest, ("interval",))
            post_target = flushes_during(post_burst, args.burst_dest, TARGET_TRIGGERS)
            results.append(
                (
                    pre_interval > 0 and post_target > 0,
                    f"burst tenant {args.burst_dest} trigger migration: interval x{pre_interval:.0f} pre-burst, "
                    f"target-triggered x{post_target:.0f} post-burst (want both > 0)",
                )
            )
            burst_lag_max = max((s.lag.get(args.burst_dest, 0.0) for s in post_burst), default=0.0)
            results.append(
                (
                    burst_lag_max <= 2 * args.flush_interval,
                    f"burst tenant lag through the shift: max {burst_lag_max:.0f}s "
                    f"(want <= {2 * args.flush_interval:.0f}s)",
                )
            )

    # soft: sliced trigger observed for head
    sliced = flushes_during(all_snaps, args.head, ("sliced",))
    if sliced == 0:
        print("WARN: no 'sliced' flushes observed for head — backlog slicing not exercised this run")

    print(f"\n=== loadcheck results (target unit: {unit}) ===")
    failed = 0
    for ok, msg in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {msg}")
        failed += 0 if ok else 1
    print("=== overall: " + ("PASS" if failed == 0 else f"FAIL ({failed} hard assertion(s))") + " ===")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
