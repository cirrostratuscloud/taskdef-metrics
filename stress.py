#!/usr/bin/env python3
"""Stress-test ECS task definitions as a free, append-only metrics store.

The bit: CloudWatch costs money; ECS task-definition revisions are free control-
plane storage. This registers a new revision of one task-def family roughly once
per second, stashing a timestamped "metric" datapoint in the container env vars,
so the revision history reads back like a time series.

Behavior:
  - Registers revisions in a loop, targeting ~1/sec.
  - Waits for each RegisterTaskDefinition to actually succeed before pacing the
    next one (no fire-and-forget). If we get throttled, back off exponentially
    with jitter and RETRY THE SAME datapoint until it lands.
  - Prints live progress; on Ctrl-C prints a summary (revisions written, elapsed,
    effective rate, throttle count, highest revision number reached).

Usage:
    python stress.py                         # defaults: family=metric-stress, forever
    python stress.py --family cpu --limit 500
    python stress.py --region eu-west-1 --interval 1.0
    python stress.py --metric-name cpu_pct   # what the fake datapoint is called

Read the "time series" back with:
    aws ecs list-task-definitions --family-prefix metric-stress --sort DESC
    aws ecs describe-task-definition --task-definition metric-stress:123 \
        --query 'taskDefinition.containerDefinitions[0].environment'

Cleanup (revisions can only be deleted after being deregistered, and even then
delete is best-effort — this is part of the joke):
    python stress.py --cleanup --family metric-stress

WARNING: revisions are effectively permanent. Deregister makes them INACTIVE;
DeleteTaskDefinitions only works on INACTIVE ones and isn't always available.
You will leave a large, near-undeletable pile of revisions behind. That's the
experiment. Run in a throwaway account/region.
"""

import argparse
import math
import random
import signal
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Error codes AWS uses for throttling across services.
THROTTLE_CODES = {
    "ThrottlingException",
    "Throttling",
    "ThrottledException",
    "TooManyRequestsException",
    "RequestLimitExceeded",
    "ClientLimitExceededException",
}


def make_client(region):
    # Disable botocore's built-in retries so WE control backoff and can measure
    # throttling honestly rather than having the SDK silently retry for us.
    cfg = Config(retries={"max_attempts": 0, "mode": "standard"}, region_name=region)
    return boto3.client("ecs", config=cfg)


def fake_datapoint(metric_name, seq):
    """A plausible-looking metric value + real timestamp, stored in env vars."""
    now = datetime.now(timezone.utc)
    # A wobbly sine wave so the "time series" looks alive when you read it back.
    value = round(50 + 40 * math.sin(seq / 12.0) + random.uniform(-5, 5), 2)
    return [
        {"name": "K53_METRIC", "value": metric_name},
        {"name": "K53_VALUE", "value": str(value)},
        {"name": "K53_TS", "value": now.isoformat()},
        {"name": "K53_EPOCH", "value": str(int(now.timestamp()))},
        {"name": "K53_SEQ", "value": str(seq)},
    ], value, now


def register_once(ecs, family, metric_name, seq):
    """Register one revision. Returns (revision_number, value, ts, latency_s).

    Blocks/retries on throttling with exponential backoff + jitter until it
    actually succeeds.
    """
    env, value, ts = fake_datapoint(metric_name, seq)
    container = {
        "name": "metric",
        # No image is pulled — we never run this task def, we just store it.
        "image": "scratch",
        "cpu": 0,
        "memory": 16,
        "essential": True,
        "environment": env,
    }

    attempt = 0
    start = time.monotonic()
    while True:
        try:
            resp = ecs.register_task_definition(
                family=family,
                containerDefinitions=[container],
                # tiny, since we never run it
                requiresCompatibilities=["EC2"],
                networkMode="bridge",
            )
            td = resp["taskDefinition"]
            latency = time.monotonic() - start
            return td["revision"], value, ts, latency, attempt
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in THROTTLE_CODES:
                attempt += 1
                # exp backoff capped at 30s, full jitter
                backoff = min(30.0, 0.5 * (2 ** min(attempt, 6)))
                sleep = random.uniform(0, backoff)
                print(f"  throttled ({code}), backoff {sleep:0.2f}s (attempt {attempt})",
                      file=sys.stderr, flush=True)
                time.sleep(sleep)
                continue
            # Non-throttle error: surface it.
            raise


def cleanup(ecs, family):
    """Best-effort teardown: deregister all ACTIVE revisions, then try to delete.

    This is deliberately included to demonstrate how hard these are to remove.
    """
    print(f"cleanup: listing ACTIVE revisions for family {family} ...")
    paginator = ecs.get_paginator("list_task_definitions")
    arns = []
    for page in paginator.paginate(familyPrefix=family, status="ACTIVE"):
        arns.extend(page.get("taskDefinitionArns", []))
    print(f"cleanup: {len(arns)} ACTIVE revisions to deregister")

    deregistered = 0
    for arn in arns:
        try:
            ecs.deregister_task_definition(taskDefinition=arn)
            deregistered += 1
            if deregistered % 50 == 0:
                print(f"  deregistered {deregistered}/{len(arns)}")
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in THROTTLE_CODES:
                time.sleep(random.uniform(0.5, 2.0))
                try:
                    ecs.deregister_task_definition(taskDefinition=arn)
                    deregistered += 1
                except ClientError:
                    pass
            else:
                print(f"  deregister failed for {arn}: {code}", file=sys.stderr)
    print(f"cleanup: deregistered {deregistered} revisions (now INACTIVE)")

    # Try the actual delete (only works on INACTIVE, in batches of 10).
    try:
        inactive = []
        for page in paginator.paginate(familyPrefix=family, status="INACTIVE"):
            inactive.extend(page.get("taskDefinitionArns", []))
        deleted = 0
        for i in range(0, len(inactive), 10):
            batch = inactive[i:i + 10]
            try:
                ecs.delete_task_definitions(taskDefinitions=batch)
                deleted += len(batch)
            except ClientError as e:
                print(f"  delete batch failed: {e.response.get('Error',{}).get('Code')}",
                      file=sys.stderr)
                break
        print(f"cleanup: deleted {deleted} INACTIVE revisions "
              f"({len(inactive) - deleted} likely un-deletable — that's the joke)")
    except Exception as e:  # delete_task_definitions may not exist / be permitted
        print(f"cleanup: delete step unavailable ({type(e).__name__}); "
              f"revisions remain INACTIVE forever", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--family", default="metric-stress", help="task-def family name")
    ap.add_argument("--region", default=None, help="AWS region (else from env/profile)")
    ap.add_argument("--interval", type=float, default=1.0,
                    help="target seconds between successful writes (default 1.0)")
    ap.add_argument("--limit", type=int, default=0,
                    help="stop after N revisions (0 = run until Ctrl-C)")
    ap.add_argument("--metric-name", default="cpu_pct", help="fake metric name")
    ap.add_argument("--cleanup", action="store_true",
                    help="deregister+delete all revisions for the family, then exit")
    args = ap.parse_args()

    ecs = make_client(args.region)

    if args.cleanup:
        cleanup(ecs, args.family)
        return

    stop = {"flag": False}

    def handle_sigint(sig, frame):
        stop["flag"] = True
        print("\nstopping after current write ...", flush=True)

    signal.signal(signal.SIGINT, handle_sigint)

    print(f"stressing task-def family '{args.family}' in region "
          f"{ecs.meta.region_name} — target {args.interval:.1f}s/write, "
          f"{'∞' if args.limit == 0 else args.limit} writes. Ctrl-C to stop.\n")

    written = 0
    throttle_events = 0
    first_rev = None
    last_rev = None
    run_start = time.monotonic()
    seq = 0

    try:
        while not stop["flag"]:
            loop_start = time.monotonic()
            seq += 1
            rev, value, ts, latency, attempts = register_once(
                ecs, args.family, args.metric_name, seq)
            written += 1
            throttle_events += attempts
            if first_rev is None:
                first_rev = rev
            last_rev = rev

            print(f"[{written:>6}] {args.family}:{rev}  "
                  f"{args.metric_name}={value:<7}  {ts.isoformat()}  "
                  f"(api {latency*1000:6.0f}ms"
                  f"{', ' + str(attempts) + ' throttles' if attempts else ''})",
                  flush=True)

            if args.limit and written >= args.limit:
                break

            # Pace to the target interval, accounting for how long the (blocking,
            # retried) write actually took. If it took longer than interval, go
            # immediately.
            elapsed = time.monotonic() - loop_start
            if elapsed < args.interval:
                time.sleep(args.interval - elapsed)
    finally:
        dur = time.monotonic() - run_start
        rate = written / dur if dur > 0 else 0
        print("\n" + "=" * 60)
        print("SUMMARY")
        print(f"  revisions written : {written}")
        print(f"  revision range    : {first_rev} .. {last_rev}")
        print(f"  elapsed           : {dur:0.1f}s")
        print(f"  effective rate    : {rate:0.2f} writes/sec")
        print(f"  throttle retries  : {throttle_events}")
        print("=" * 60)
        print(f"read it back:\n  aws ecs describe-task-definition "
              f"--task-definition {args.family}:{last_rev} "
              f"--query 'taskDefinition.containerDefinitions[0].environment'")


if __name__ == "__main__":
    main()
