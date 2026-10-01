# Next Graph500 capacity qualification

The closed compact-runtime Pecan SSSP DeltaStar scale-24 run used 33.05 GiB
at its whole-container peak. Scale 25 doubles the graph to 33,554,432 vertices
and 536,870,912 undirected input tuples. Doubling that peak gives **66.1 GiB
as a planning estimate**, not a prediction or a demonstrated bound. This makes
a controlled attempt within the existing 100 GiB container limit worthwhile.

Grenada's weighted relational path also uses the GraphAlgorithms controller,
so it needs the same compact-runtime qualification. Its previous scale-25 BFS
success does not establish weighted SSSP capacity. The fresh small Grenada
DeltaStar smoke passed independent heap-Dijkstra and parent checks for all
16,384 vertices, converged in 37 iterations, exited cleanly, and recorded no
OOM events. See [the closure](smoke-closure.json) and its complete compressed
[host diagnostic bundle](smoke-evidence.tar.gz). This fixture is not Graph500.

## Declared sequence

| Order | Cell | Condition |
|---|---|---|
| 1 | Pecan scale 25 SSSP DeltaStar | Identity and resource admission pass |
| 2 | Grenada scale 24 SSSP DeltaStar | Prior workload closed; admission passes again |
| 3 | Grenada scale 25 SSSP DeltaStar | Grenada scale 24 correctness and closure pass; admission passes again |

The machine is shared Morrobay: one Linux VM, two Sail workers, 32 CPUs,
32 partitions, 32 threads, and 64 task slots per worker. Each container has a
100 GiB hard memory limit and no additional swap allowance. The 96 GiB Sail
pool and 80 GiB native quota are per process and do not form a combined bound.
These cells qualify capacity and correctness. They do not establish cluster
scaling, a dedicated-host runtime, or a matched performance improvement.

The source is vertex 13,507,776, direction is undirected, delta is 0.1, the
algorithm cap is 1,000 iterations, and the certificate cap is 10,000 rounds.
Existing scale-24/25 inputs are reused through fresh symlink namespaces;
manifest hashes and canonical edge hashes are pinned in [plan.json](plan.json)
and the three configurations. The producer also verifies the dataset.

The new algorithm timeout is eight hours; the whole-cell outer timeout is
nine hours, including startup and certificate work. The prior scale-24 cell
used shorter deadlines. This changed capacity budget is explicit; a timeout
remains a timeout, and an interrupted result is not a completed measurement.

## Fixed implementation

| Component | Source revision |
|---|---|
| Compact Sail runtime | `56194b170155301ba91077f0ba3df31fe2c78b6b` |
| Controller and benchmark harness | `3a9028057c6c6c5034492845926fc4bc18f9626f` |
| Native extension wheel | `ffcfbd5690e3f3681ef9ac18ba959bc231cf9f73` |

The runtime and native binary hashes, clean container source revision, all
host-side Python support hashes, dataset manifests, and VM boot identity are
checked again before every launch. The later native memory fixes and the
controller changes with unfavorable small-cell results are not combined into
this experiment. The exact configurations retain the earlier source logging,
physical plans, keepalive and stream-creation settings.

## Resource supervision and evidence

[supervise.py](supervise.py) runs cells serially through the unchanged pinned
`run_focused_safe.py`. Admission requires no other running container, at least
102 GiB guest available memory, and at least 32 GiB free volume space for scale
25 or 30 GiB for scale 24. These are admission thresholds, not storage bounds.
The readiness observation had 34.70 GiB free volume space; a larger staging
peak can still exhaust the budget. Existing datasets and outputs are retained.

The supervisor polls free volume space, VM identity, running containers and
host paging/load, recording its observations. A disk reading below 8 GiB,
foreign-container interference, an observer failure or an identity change
ends the queue and stops only the supervisor's verified workload. Polling has
gaps and is not a storage reservation. A second running job can also appear
between observations. The fixed lock coordinates these supervisors only.

An ordinary, completely collected non-pass result is retained and allows the
independent next cell to seek fresh admission. Missing receipts, uncertain
cleanup and guard stops prevent further launches. The wrapper returning zero
does not count as success: qualification checks the producer result,
certificate, identities, container exit, OOM counters and cleanup. A separate
physical Parquet domain/row scan remains required before publication of each
large successful result, and is explicitly marked pending by the supervisor.

The remote evidence root is
`/Users/alexy/src/sail-extensions-gates/stream-experiments-20260930` on Morrobay.
Each named cell retains its configuration, receipt, plans, logs and collection
record there. Full output and staging files remain in the Docker volume. The
supervisor's directory is `next-graph500-20261001-supervisor`; its final receipt
is authoritative for the queue state. Preparation of a configuration is not
evidence that its cell has launched or passed.

## Argentea is a separate diagnosis

[Why Argentea can fail early](ARGENTEA-EARLY-FAILURES.md) separates the successful
scale-24 BFS, the scale-24 SSSP round-cap exhaustion, the scale-25 BFS OOM retry,
and the unexplained two-host ingestion cancellation. It quantifies the initial
topology traffic without equating traffic volume to simultaneous memory, and
identifies the queue/phase counters needed to attribute the later memory peak.

The preceding [compact replay](../sail-stream-experiments-2026-09-30/COMPACT-REPLAY-AND-SSSP.md)
and [cluster preparation review](../sail-stream-experiments-2026-09-30/CLUSTER-PREPARATION.md)
remain the evidence and design context. These next capacity cells cannot by
themselves demonstrate near-linear distributed scaling.

## Launch status

The unchanged supervisor was launched on Morrobay after the four WCC pilot
cells closed, using explicit host Python 3.12. The fresh prelaunch observation
found 90.95 GiB free volume space and 106.23 GiB guest available memory,
unchanged VM identity and no running containers. See [prelaunch evidence](prelaunch-after-wcc.json)
and [launch receipt](launch01.json). This records a launch, not a completed cell;
the supervisor still applies its per-cell admission and continuous guards.

Status written UTC: 2026-10-01T05:59:41.897030+00:00
