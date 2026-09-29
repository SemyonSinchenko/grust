# Graph Nuts capacity campaign, 2026-09-29 (in progress)

Where the four Graph Nuts paths stop on Graph500 scale 24, 25 and 26 and on
cit-Patents, on morrobay's gate VM (32 CPUs, 110 GiB, containers of 32 CPUs
and 100 GiB; Sail memory pool 96 GiB per process, native quota 80 GiB, cell
timeout 5400 s). A shared, virtualized host: every timing here is an
observation, not a publishable number. Every refusal, error and timeout is a
result and is kept with its receipt. The map is
[`GRAPH-NUTS.md`](../GRAPH-NUTS.md); the scaling plan is
[`FABLE-ON-ASTRA.md`](../FABLE-ON-ASTRA.md).

Evidence root on morrobay: `~/src/sail-extensions-gates/graph-nuts-b87fb27ac/`
(baseline) and `~/src/sail-extensions-gates/graph-nuts-gate-next/` (the new
gate, the chain logs, `capacity_findings.py`, `pick_sources.py`). Volume paths
are under `/targets/`.

## 1. Inputs

| Input | Vertices | Edges | Files | Source vertex | Why that source |
|---|---|---|---|---|---|
| Graph500 scale 24 (seeds 42/54, edge factor 16) | 16,777,216 | 268,435,456 | 1024 | 13507776 | highest sampled degree (46,207 in 1/16 of the edges); vertex 0 is isolated |
| Graph500 scale 25 | 33,554,432 | 536,870,912 | 2048 | 13507776 | same vertex is the top of the scale-25 sample too (34,993) |
| Graph500 scale 26 | 67,108,864 | 1,073,741,824 | 4096 | `max-degree` (fixture-chosen) | the new fixture policy records the source's degree |
| cit-Patents (SNAP, pinned `d2a11214…`) | 3,774,768 | 16,518,948 | 64 | 3569341 (dense id) | highest out-degree, 770 citations; directed |

The first capacity matrix used `source: 0`, copied from the harness example.
Vertex 0 is isolated in these Kronecker graphs: the one passed cell reported
`reached = 1` after one empty round, so it measured graph loading, not
traversal. That matrix was stopped after five cells (kept under
`capacity/`, with `STOPPED-2026-09-29.md`), the inputs were re-prepared with
the sources above (the fixtures pin `traversal.source` in the manifest and
the cell asserts it), and the harness now has `--source max-degree` and
records the source's degree and the reached count in every manifest and
summary (`work/s0-source-degree`).

## 2. What the baseline (`b87fb27ac`) could not do at scale 25

Both found by the first two source-0 cells, before any traversal ran.

- **Banda refuses to stage.** The canonical staging sort admits its working
  space from the memory budget before sorting: 8.6 GB of permutation, about
  396 GB of sort keys, 141 GB for the sorted copy, 404 GB in all against the
  80 GiB quota; refused in 92 s at scale 25 and 65 s at scale 24. The
  sort-key bound (`admission.rs`: 16 times the key buffers plus 128 bytes per
  row per key column) is about 25 times the Arrow row-format size for these
  short Utf8 ids. The library already offers `order = asStaged`, but the Sail
  extension hard-coded canonical and its request schema rejected the option.
  Fix on `work/s2-stage-order`; the next matrix runs Banda `asStaged`.
- **Relational cells die on the first iteration.** `decoded message length
  too large: found 8234561 bytes, the limit is: 4194304 bytes`: Sail's
  internal gRPC clients keep Tonic's 4 MiB decode default while its servers
  accept 128 MiB. Scale 24 (1024 files) passed the same path. Fix on
  `work/grpc-client-decode-limit` (one hunk, upstream candidate, verification
  pending on the next matrix).

## 3. Baseline matrices with the hub source (running)

`gn-capacity-b87fb27a-hub` (36 cells: BFS reference/frontier/push_pull and
SSSP reference/frontier/delta_star, Pecan, Banda and Grenada, scale 24 and
25) and `gn-ranking-b87fb27a-hub` (30 cells on cit-Patents: PageRank and WCC
reference/optimized under the certificate policy, plus the traversal cells).
Results are filled in from `capacity_findings.py` as cells finish; this table
is the state at 12:00 UTC (16 of 36 cells finished; two BFS cells remain, Banda reference and Grenada frontier at scale 25, then the 18 SSSP cells).

| Cell | Outcome | Time | Peak PSS | What happened |
|---|---|---|---|---|
| scale 25, Banda BFS push_pull (canonical) | refused | 112 s | 25.6 GiB | the staging sort's admitted working space, as in section 2 |
| scale 25, Grenada BFS push_pull | error | 1516 s | 38.6 GiB | six real BFS iterations from the hub, then `decoded message length too large: found 8233665 bytes` (the same message size as the source-0 run, so it does not depend on the frontier) |
| scale 24, Pecan BFS frontier | error | 465 s | 55.4 GiB | iteration 1 reached 407,203 active vertices; during iteration 2 the driver lost a worker connection (`h2 protocol error: error reading a body from connection`, worker 2 `ConnectionReset`); no OOM kill (cgroup peak 60 GiB of 100, workers at 27.4 and 23.7 GiB RSS); cause not identified from the driver log, which carries no worker output |
| scale 24, Grenada BFS push_pull | passed | 775 s | | six iterations from the hub, 8,862,601 of 16,777,216 vertices reached (the giant component), certificate validated |
| scale 24, Banda BFS frontier (canonical) | refused | 58 s | 13.8 GiB | staging sort admission, as at scale 25 |
| scale 25, Pecan BFS push_pull | error | 1610 s | | six real iterations, then the 4 MiB client limit with an 8,234,369-byte message, the same size as Grenada's, so the message is tied to the scale-25 input, not the path or the frontier |
| scale 25, Banda BFS frontier (canonical) | refused | 125 s | | staging sort admission |
| scale 24, Banda BFS push_pull (canonical) | refused | 56 s | | staging sort admission |
| scale 25, Grenada BFS reference | error | 2730 s | 100 GiB | iteration 1 reached 640,062, iteration 2 reached 14,625,247 (932 s); iteration 3, relaxing all 15M reached vertices, drove the container to its 100 GiB limit (1610 `max` events, no OOM kill) with the workers at 42.8 and 43.3 GiB, and the driver lost the stream (`h2 protocol error`) |
| scale 24, Banda BFS reference (canonical) | refused | 60 s | | staging sort admission |
| scale 24, Grenada BFS reference | error | 480 s | 50.8 GiB | iteration 1 reached 407,203; iteration 2 failed with `h2 protocol error: error reading a body from connection` at a 51 GiB container peak with no `memory.max` events at all, so this one is not memory |
| scale 24, Pecan BFS push_pull | passed | 844 s | | six iterations, 8,862,601 reached, the same result as Grenada's push-pull (775 s); certificate validated |
| scale 24, Grenada BFS frontier | error | 786 s | 99.9 GiB | iteration 1 reached 407,203; iteration 2 ended with the `h2 protocol error` at the container limit (workers at 43.3 and 48.1 GiB, no `memory.max` event counted, no OOM kill) |
| scale 25, Pecan BFS frontier | **passed** | 2729 s | 75.1 GiB PSS, container peak 99.7 GiB | the baseline's first scale-25 pass: frontiers 640,062 / 14,625,247 / 1,777,122 / 6,267 / 28 / 0 over six iterations (iteration 2 alone took 949 s), 17,048,727 of 33,554,432 reached, certificate validated with 5 witness rounds; the workers peaked at 34.5 and 39.7 GiB, so it passed within about 300 MiB of the container limit |
| scale 25, Pecan BFS reference | error | 3144 s | 100 GiB | got through three iterations (frontiers 640,062 / 14,625,247 / 1,777,122, iteration 3 took 1197 s), then iteration 4, relaxing all 17M reached vertices, drove the container to its limit (3767 `max` events, no OOM kill; workers at 45.4 and 38.9 GiB) and the driver lost the stream |
| scale 24, Pecan BFS reference | error | 793 s | 99.9 GiB | iteration 1 reached 407,203; during iteration 2 the container hit its 100 GiB limit and the kernel OOM-killed a worker (`memory.events oom_kill 1`); the two workers were at 47.2 and 46.3 GiB RSS |

Confounder for these four cells: the default Colima VM (12 CPUs, 48 GiB) had
come back up at about 05:02 UTC beside the 110 GiB gate VM on the 128 GB
host (2.4 GB of host swap in use); it was idle and was stopped again at
06:25 UTC. Whether the worker loss is related is not known.

### The `h2 protocol error` failures

Four relational cells ended with `h2 protocol error: error reading a body
from connection` in the driver: Pecan frontier at scale 24 (60 GiB peak,
iteration 2), Grenada reference at scale 24 (51 GiB, no `memory.max` events,
iteration 2), Grenada frontier at scale 24 (99.9 GiB, iteration 2) and
Grenada reference at scale 25 (100 GiB, iteration 3). At least the Grenada
reference at scale 24 is not memory; the others sit near the limit, so both
causes may be in play. The worker processes leave no log lines in the
driver's log (they never initialize logging), so the worker side is
invisible; the working hypothesis is that a worker's own shuffle read hit the
same 4 MiB client decode limit (a worker is a Flight client of its peer), its
task stream ended abruptly, and the driver reported the broken body instead
of the limit message. The next matrix reruns exactly these cells (scale-24
relational reference and frontier, BFS and SSSP, both engines) with the
client limit raised; if they pass, that is the cause.

### Pecan's second iteration at scale 24

Both Pecan BFS cells so far (frontier and reference) failed in iteration 2,
the expansion of the hub's 407,203 first-level neighbors, with the two worker
processes at 47 GiB each (reference, OOM-killed) or 27 and 24 GiB (frontier,
connection lost at 60 GiB). Grenada's push-pull BFS on the same materialized
adjacency passed at a 25 GiB container peak. The relational expansion is
`adjacency.join(active, adjacency.src == active.id)` over the materialized
undirected adjacency (536,870,912 rows at scale 24, both directions). A local
explain on Capitola (`scratchpad/join-side/explain.py`, single-process mode,
1.6M-edge adjacency, 1-row frontier) shows DataFusion choosing the frontier as
the hash-join build side in either join order (`HashJoinExec: mode=CollectLeft`
with the frontier as the left input), so at small scale the planner does not
build on the adjacency. What the workers hold at scale 24 in process-cluster
mode, where the join is partitioned and both inputs are shuffled, is not
established by these receipts: the harness records no plan. Next step for S5:
the gate now records the cluster-mode physical plan of the expansion join
per iteration (`work/s5-iteration-plans`); compare push-pull's pull-side
joins with the reference join at the same frontier from the scale-25 receipts.

## 4. The new gate (`work/s5-iteration-plans`, `edcf86824`)

Built from the fork after the baseline matrices: `work/s0-tiered-accounting`
plus the stage-order passthrough, the client decode limit, the `max-degree`
source policy, an `argentea` harness engine, and per-iteration plan
recording for the relational paths (`--record-plans`), so the scale-25
relational receipts carry the physical plan of every expansion join. The Argentea engine passed a
Capitola smoke before being queued: five methods (BFS reference, frontier,
direction; SSSP reference, delta_star) on a 2000-vertex directed fixture in
process-cluster mode, all validated against the independent reference, about
7 s each, 1999 of 2000 reached.

Matrix `gn-capacity-edcf8682` (42 cells): the 12 scale-25 relational cells
again, 8 scale-24 relational reference/frontier reruns, 12 Banda `asStaged`
cells at scale 24 and 25, and 10 Argentea cells (30-round cap, 32
partitions). Then `gn-capacity-scale26-edcf8682` (23
cells) on scale 26.

_Pending._

## 5. Findings so far

0. The relational paths can traverse Graph500 scale 25 on this envelope, but
   barely: Pecan's frontier BFS reached 17.0M vertices in 45 minutes with the
   container within 300 MiB of its 100 GiB limit, while the reference variant
   (all reached vertices relaxed each round) and both push-pull paths failed
   at scale 25 for the two reasons in section 2 and section 3.

1. A benchmark fixture's default source has to be checked for degree zero;
   the harness now refuses to let that pass silently.
2. Two of the three Graph Nuts paths were blocked at scale 25 by fixed
   limits, not by the graph: an admission bound that overestimates the sort's
   keys by an order of magnitude, and a transport default the servers had
   already outgrown. Both are one-place fixes.
3. Argentea runs through the same harness as the other paths, so the four-way
   comparison the plan asks for can now be one matrix.
