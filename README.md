# taskdef-metrics — CloudWatch is expensive, ECS task-def revisions are free

A stress test / proof-of-concept for storing timestamped "metrics" as ECS task
definition **revisions**. Each revision stashes a datapoint (value + ISO
timestamp) in the container `environment`, so the revision history reads back
like a (write-once, append-only) time series — for $0 of CloudWatch.

## Run

```bash
# once per second, forever, until Ctrl-C
python stress.py --region eu-west-1

# bounded run
python stress.py --region eu-west-1 --limit 500 --metric-name cpu_pct
```

It targets ~1 write/sec, **waits for each RegisterTaskDefinition to actually
succeed** before pacing the next, and backs off (exp + jitter) on throttling,
retrying the same datapoint until it lands. Ctrl-C prints a summary.

## Read the "time series" back

```bash
aws ecs list-task-definitions --family-prefix metric-stress --sort DESC
aws ecs describe-task-definition --task-definition metric-stress:123 \
  --query 'taskDefinition.containerDefinitions[0].environment'
```

## What we're actually testing

- How high do revision numbers go? (kube53 hit 951 by accident; here we do it on
  purpose.) There's no documented hard cap — revisions increment indefinitely.
- How hard does `RegisterTaskDefinition` throttle at ~1/sec sustained?
- How un-deletable is the resulting pile? (`--cleanup` demonstrates: deregister
  → INACTIVE, and `DeleteTaskDefinitions` only works on INACTIVE and is
  best-effort. You will leave revisions behind. That's the point.)

## Cleanup (best-effort, on purpose)

```bash
python stress.py --cleanup --family metric-stress
```

> ⚠️ Run in a throwaway account/region. Revisions are effectively permanent;
> this deliberately creates a large, near-undeletable pile. It's a joke about
> free control-plane storage, not a real metrics backend.
