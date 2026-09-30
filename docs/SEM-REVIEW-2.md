# Sem's second review: relational execution measurements and a qualification plan

Written 2026-09-30 for review by Astra and Sem. It records Sem's numbers
and his argument, sets ours beside them, separates what is measured from
what is read from source and what is still unmeasured, and proposes a
staged plan in which every stage has a change, a control, an acceptance
number and a stop rule. It builds on
[`GRAPHFRAMES-RS-PARITY.md`](GRAPHFRAMES-RS-PARITY.md) (2026-09-28: where
graphframes-rs gets its speed, and the declared-layout work), on the
campaign record
[`reviews/gn-capacity-2026-09-29.md`](reviews/gn-capacity-2026-09-29.md)
section 4b, and on Astra's review and experiments of today
([`reviews/sail-graphs-2026-09-30/REVIEW.md`](reviews/sail-graphs-2026-09-30/REVIEW.md),
[`reviews/sail-stream-experiments-2026-09-30/RESULTS.md`](reviews/sail-stream-experiments-2026-09-30/RESULTS.md),
[`CLUSTER-PREPARATION.md`](reviews/sail-stream-experiments-2026-09-30/CLUSTER-PREPARATION.md)).
Sem's first review, his PR 30 and his objection to building a CSR at all,
is answered in [`FABLE-ON-ASTRA.md`](FABLE-ON-ASTRA.md) section 8.

No cell was run for this document. The gate was building
`work/stream-performance-review` when it was written.

## 1. What Sem says

Translated from his messages of 2026-09-30:

- His benchmarks are finishing; the numbers "should be 100% achievable in
  Sail. A constant overhead for plan serialization, the Python loop and so
  on cannot be avoided, but you must get about the same performance class."
- "On a nominal 30 GB machine it must survive, and if you add resources it
  must perform better (more memory, less spill)."
- "If WCC on cit-Patents runs 700 seconds for you on a 32-core machine,
  something is 100% wrong, because for me it is 4.7 seconds on a machine
  with 16 cores."
- "Same class simply because both are just DataFusion, and the whole
  Connect overhead adds about a constant per iteration (ser-de, gRPC, plan)
  that does not depend on data size, since the plan is the same for any
  graph."
- "If same class does not come out, or if it needs three times the memory
  to be same class, I would suggest rethinking all of it from scratch."
- Expected in Sail: "on the order of 10 seconds for cit-Patents and 30 to
  40 minutes for graph500-28."
- On spill: his runs spill too ("120 GB peak disk for WCC on graph500-28"),
  so "if similar numbers do not come out in Sail, with a small constant
  correction for Sail's overhead, then the problem is not only spill."

His results (`SemyonSinchenko/graphframes-rs`, branch
`new-benchmark-results`, `benches/results`; c5d.4xlarge, 16 vCPUs, 32 GiB,
`--max-memory 30G --num-workers 16`; wall time / peak RSS / peak disk,
medians of 5):

| Graph | WCC | PageRank, 10 iterations | Shortest paths |
|---|---|---|---|
| cit-Patents (3.77M vertices, 16.5M edges) | 4.71 s / 1.46 GB / 0.23 GB | 4.05 s / 1.06 GB / 0.33 GB | 0.90 s / 0.95 GB |
| graph500-24 (8.87M non-isolated vertices, 260M edges) | 33.3 s / 14.0 GB / 6.2 GB | 24.5 s / 5.0 GB / 2.3 GB | 6.7 s / 5.0 GB |
| graph500-25 | 82.5 s / 18.6 GB / 12.2 GB | 62.3 s / 12.4 GB / 8.6 GB | 26.4 s / 12.4 GB |
| graph500-26 | 213 s / 20.5 GB / 28.8 GB | 149 s / 14.3 GB / 19.3 GB | 72.9 s / 13.8 GB |
| graph500-28 (121M vertices, 4.24G edges) | 1009 s / 20.2 GB / 120 GB | 912 s / 18.3 GB / 91.4 GB | 783 s / 17.6 GB |

`--num-workers` is DataFusion's `target_partitions` in one process;
`--max-memory` is the spill pool. His WCC follows Bögeholz, Brand and
Todor (ICDE 2020), randomized contraction, with Parquet checkpoints
written and re-read between iterations.

## 2. Ours, and the size of the gap

Baseline `b87fb27ac`, Linux gate, 32 cores, 100 GiB container, Sail in
process-cluster mode (driver and two worker processes, 32 partitions):

| cit-Patents | Ours | Against Sem |
|---|---|---|
| Pecan WCC, randomized contraction | 312 s, 19 rounds | 66 times |
| Pecan WCC, min-label | 500 s, 20 rounds | 106 times |
| Grenada WCC (the same controller over graph tables) | 397 and 566 s | 84 and 120 times |
| Pecan PageRank power | 729 s, 20 iterations (setup 58 s, first iteration 23 s) | about 80 times per iteration |
| Pecan BFS | 164 s | 180 times his shortest-paths run |
| Banda WCC | 30 to 39 s: staging 13.8, projection 14.4, kernel and output 1.4 to 10.6 | 6 to 8 times |
| Banda PageRank power | 32 s: 13.8 + 14.6 + 3.3 | 8 times |

At scale 24 the relational path completes only push-pull BFS (775 to
844 s, against his 6.7 s shortest paths), and the setup alone, measured by
the two cells whose source was isolated, is 200 s (Pecan) and 372 s
(Grenada).

Sem's spill point holds against our own receipts. Every cit-Patents
ranking cell peaked at 2.8 to 4.0 GiB in a 100 GiB container, so nothing
spilled, and those are the cells that are 66 to 120 times his. Spill is a
cost he pays at graph500-28 and still finishes in 17 minutes; it is not
what separates us on the small graph.

Memory, his other criterion: our cit-Patents cells use 2 to 4 times his
peak RSS (2.8 to 4.0 GiB against 1.06 to 1.46 GB). At scale 24 the
relational reference and frontier variants reach 40 to 100 GiB where his
WCC peaks at 14 GB.

## 3. Where the time goes

Each factor is marked **measured** (a receipt or a recorded run), **source**
(read from code, no timing attached) or **unmeasured**.

**F1. Every campaign cell ran Sail as a cluster; his run is one process.**
*Measured, on different hosts.* No cell of the campaign used
`--mode local`. The only local-mode measurement we have is from the
declared-layout work (parity document, section 13): Pecan PageRank on 2M
vertices and 16M edges, local mode, four partitions, one process on
Capitola, 2.9 s per round and 12.5 s of setup. The gate's cluster-mode
cell on cit-Patents, a graph of the same edge count, is 23 to 34 s per
round and 58 s of setup. That is about ten times between the two modes,
across two machines and two graphs, so it is an indication and not a
ratio. Sem's is 0.4 s per iteration.

**F2. A round is several Spark Connect jobs, not one plan.** *Source; the
floor is measured.* `StagingRun.materialize`
(`pyspark_pecan/staging.py`) does a keyless `repartition(P)`, a Parquet
write, a read back, and, where the caller expects a row count, a
`count()` job. Randomized WCC (`wcc_randomized.py`, the non-fused plan the
matrix ran) materializes three tables per round and issues three more
counts for its metrics and loop test: seven to nine jobs. Min-label does
the join and aggregate, one materialize with its count, and a second full
join only to ask whether any label changed. PageRank power issues the
dangling-mass scalar, the state write with its count, and the convergence
scalar. The measured floor: the last eleven rounds of randomized WCC, on a
contracted graph that is almost empty, take 4.6 to 5.7 s each. Nineteen
rounds of floor are about 90 s, nineteen times his whole run. Sem's
"constant per iteration" exists, and for us it is about 5 s.

**F3. Setup before the first round.** *Measured.* `_snapshot`
(`algorithms.py`) rewrites vertices and edges to Parquet, then runs five
validation jobs (null ids, id uniqueness by group-by, null endpoints, two
anti-joins of every edge against the vertices) and a count: 28 s before
randomized WCC's first round, 39 s before min-label, 58 s before
PageRank. His complete runs are shorter than our setup.

**F4. The edge relation is re-shuffled every round.** *Source, and
measured in the parity work.* Nothing tells the optimizer that a
checkpoint is partitioned and sorted by its key, so every join
repartitions both sides: three hash shuffles per round. graphframes-rs
declares the layout and joins without a shuffle, which Sem puts at three
times. Our state: the read side is done and verified (declared scans,
the host restatement, shuffle-free joins); the write side is correct but
slow, because a bucketed, sorted write on Sail costs 8 to 20 times a plain
write (`partitionBy` 11.6 to 12.5 s against 1.5 to 5.8 s for 16M rows),
so in local mode the declared layout came out slower than the shuffle it
removes (4.95 s against 2.9 s per round). In cluster mode the shuffle is
more expensive and the comparison has not been made.

**F5. Aggregation and estimation costs inside the engine.** *Measured in
isolation by Astra.* DataFusion 55.1's grouped `min(struct(...))`, which
Pecan's traversals use, retains about 2 KB per group and scans scratch
for every resident group on every batch; the compact accumulator on
`work/compact-struct-min` (`56194b170`) retains 48.8 times less at 100,000
groups. Not yet replayed on the gate, and no whole-query gain is claimed.
Separately, the aggregate's output is estimated at O(E) rows, which
drives join-side and broadcast choices (parity document, section 8).

**F6. Memory limits that cannot make the engine spill.** *Measured by
Astra.* The harness gives each of three processes a 96 GiB pool inside a
100 GiB container. Per-process pools do not bound the sum, so the replayed
scale-24 Pecan frontier cell was killed by the kernel with both workers
near 50 GiB instead of spilling. "Survive on 30 GB" cannot hold until the
pools add up to the limit.

**F7. Banda's time is getting the graph out of tables.** *Measured.* Its
kernels run in 1.4 to 3.3 s on cit-Patents, under his complete run; the
28 s before them is staging and projection from Utf8 ids.

What is **unmeasured** is the split between F1 and F2 on one host: how
much of a 22 s round is the cross-process exchange and how much is the
controller's extra jobs. Stage A measures it. Until then no factor above
is claimed as the cause, and Astra's caution stands: shared-host times
are observations, and a removed action is not a speedup until a paired
run shows one (the fused overflow change measured 1.09 times slower on
its first fixture).

## 4. The plan

Qualification uses a disclosed CPU, memory and disk envelope, explicit
algorithm/output contracts and independently verified results. Proposed
engineering budgets concern setup latency, jobs per round, bounded memory and
scaling efficiency. Comparative ratios are observations from matched cells,
not prescribed outcomes against another implementation.

### Stage A. Measure on his terms (no code; about two hours of gate time)

1. Build graphframes-rs at the benchmark branch in the gate image and run
   his WCC, PageRank (10 iterations) and shortest paths on cit-Patents
   and graph500-24, from the same Parquet inputs, in a 16-CPU, 32 GiB
   container. This replaces his hardware with ours in every ratio.
2. Run Pecan on the same inputs in the same container, `--mode local`,
   16 threads, 16 partitions, 30 GiB pool: WCC `randomized`,
   `randomized_fused` and `min_label`, PageRank power with 10 fixed
   iterations, BFS.
3. Run the same five in process-cluster mode with the identical total
   budget (pools split so they sum to 30 GiB) and 16 partitions.
4. For every cell record the three boundaries (algorithm ready, result
   exported, result verified), setup time, per-round time, and the number
   of jobs per round counted from the driver log. Order ABBA, twice.

Decision rule: compare local and process-cluster results only after matching
the workload and total resource envelope. Use planning, task, exchange and
checkpoint counters to choose the next isolated control. A cross-system ratio
does not identify which component caused it.

### Stage B. One job per round (client only; Pecan and therefore Grenada)

All in `examples/extensions/graph-algorithms/src/pyspark_pecan/`. Each
item is a separate commit with the 46 Pecan unit tests, the harness
certificates unchanged, and a paired ABBA measurement in Stage A's
container.

| # | Change | Where | Removes |
|---|---|---|---|
| B1 | no keyless `repartition(P)` before a checkpoint write; write the plan's own partitioning | `staging.py` `materialize` | one full shuffle per materialization |
| B2 | a write receipt instead of a read-back `count()`: the host's `gf.utils.v1` service returns rows, files and bytes from the Parquet footers of a committed stage | `staging.py`; `crates/sail-session/src/extensions/graph_utils` | one job per materialization, and the separate metric counts (`remaining`, `active_count`, `next_count`) |
| B3 | fold the round's scalars into the state write: dangling mass and the convergence sum as a one-row companion output; min-label's "changed" as a column whose footer statistic answers the loop test | `algorithms.py` `pagerank`, `wcc` | two jobs per PageRank round, one full join per min-label round |
| B4 | `randomized_fused` as the default contraction (it is the port of graphframes-rs PR 56 and exists in `wcc_fused.py`; the matrix ran the non-fused plan) | `algorithms.py`, harness `runtime.py` | the priorities table and two joins per round |
| B5 | tail cutover: below a threshold of remaining edges, finish the contraction in one plan without per-round checkpoints | `wcc_randomized.py` | the eleven floor rounds on cit-Patents |
| B6 | checkpoint every k rounds (k configurable) where the plan stays small | `staging.py`, callers | checkpoint writes in cheap rounds |
| B7 | setup in one pass: the five validation jobs as one aggregate and one anti-join, a `trusted=True` switch for inputs the caller vouches for, and no snapshot rewrite when the input is immutable Parquet | `algorithms.py` `_snapshot` | 25 to 55 s on cit-Patents |

Acceptance, local mode, cit-Patents: at most two jobs per round counted
from the driver log; per-round floor at most 1 s; setup at most 5 s;
randomized WCC end to end at most 30 s. Stop rule: if B1 to B4 together
do not bring local-mode WCC under 60 s, do not polish further; go to
Stage E.

### Stage C. Make a job cheap and a limit real (engine and harness)

| # | Change | Control | Acceptance |
|---|---|---|---|
| C1 | single-host graph runs default to local mode in the harness and the guide; cluster mode is for several hosts | Stage A's C ratio | documented with both numbers |
| C2 | the fixed cost of one distributed job: a no-op job at P = 4, 16, 32 with two workers, broken into planning and serialization, task scheduling, stream creation, teardown | the probe itself, 20 repetitions | at most 100 ms at P = 16; if above, the candidates are adaptive partition count for small shuffles and fewer streams per exchange |
| C3 | pools that sum to the container limit (driver small, workers share the rest), in `benchmarks/runtime.py` | graph500-24 WCC in a 32 GiB container | completes by spilling, no kernel kill; his run is 33 s at 14 GB with 6 GB of disk |
| C4 | the compact `min(struct)` accumulator, after Astra's Linux replay; the same allocation probe applied to `min_by`, which the fused WCC plan uses | Astra's paired method on a traversal cell | no regression in certificates; memory and time reported as ratios |

### Stage D. Stop re-shuffling the edges (the declared layout, write side)

The read side is finished (`work/declared-layout`). What remains is
section 14 of the parity document, reordered by what the campaign showed:

1. Measure the declared layout in cluster mode on the gate, where a
   shuffle crosses processes and its removal is worth more than in the
   local-mode experiment that found it slower.
2. Report Sail's slow `partitionBy` write with the existing probe
   (`layout-exp/partitionby_probe.py`); the user files it upstream.
3. Profile the driver-side writer's FFI crossing if 2 stalls.

Acceptance: a round whose only shuffle is the aggregate's, with the
checkpoint write no slower than a plain write plus 50%.

### Stage E. The loop inside the server (Sem's "rethink")

Today Grenada is Pecan's controller entered through graph tables, as
Astra's review points out; nothing in the fork runs an iterative
relational algorithm inside the engine. One architecture candidate is a single Spark Connect request per algorithm,
with the round loop in the driver:

- a driver-placed controller that plans each round and submits it as one
  job (the graphframes-rs integration plan: controller in the driver, one
  job per round through `JobRunner`), with in-engine checkpoints that
  carry their layout; or
- graphframes-rs itself as the implementation, called from a Sail
  extension on the session's DataFusion context. In local mode, the library integration and execution boundary still need verification;
  cluster mode requires an explicit per-round handoff to Sail's job runner.

Trigger: measured controller/job overhead or Stage B's disclosed stop rule. Before any code,
answer the controller-placement question the integration review asked,
and check the license and API surface of graphframes-rs at the benchmark
branch (it was Apache-2.0 at `b4da56d`).

### Stage F. Banda's ingest

S1 (Int64 identity) and S3 (dense u32 projection) from
`FABLE-ON-ASTRA.md`, with `asStaged` as the default for kernels that do
not read edge order. Acceptance: staging plus projection at most 5 s on
cit-Patents, with complete WCC and PageRank measured and verified separately.

### Order and cost

A is first and decides between B-then-C, C-first, and E. B is days of
client work and needs no host change except B2's receipt. C1 and C3 are
configuration. C2 is a day of profiling before any change is chosen. D
and F continue on their existing tracks. E is the large item and starts
only on its trigger.

## 5. Targets

Proposed engineering budgets for the disclosed Stage A envelope. They remain
unqualified until measured; the external context is not a comparative outcome
that the benchmark is required to produce.

| Input | Kernel | Sem's machine | Provisional latency/memory budget |
|---|---|---|---|
| cit-Patents | WCC | 4.7 s / 1.46 GB | 15 s / 3 GB |
| cit-Patents | PageRank, 10 iterations | 4.05 s / 1.06 GB | 12 s / 2.2 GB |
| cit-Patents | shortest paths | 0.9 s | 3 s |
| graph500-24 | WCC | 33 s / 14 GB | 100 s / 28 GB |
| graph500-24 | PageRank, 10 iterations | 24.5 s / 5 GB | 75 s / 10 GB |
| graph500-28 | WCC | 1009 s / 20 GB / 120 GB disk | 40 minutes, by spilling, on a host with the disk |

The last row needs a host with a few hundred GB of free disk; Morrobay's
volume has about 40 GiB and cannot hold it.

## 6. Questions

For Sem:

1. What does his wall time include: reading the inputs, writing the
   result, both?
2. Which WCC variant and checkpoint interval produced the table, and is
   `prefer_smj` on?
3. His graph500-24 has 8.87M vertices and 260M edges, ours 16.8M vertices
   (8.86M reached from the hub) and 268M edge tuples. Is his input
   deduplicated and stripped of isolated vertices, and can he share the
   generator command so both sides read the same bytes?
4. Would he accept Stage E's second form, his crate driving the rounds
   inside Sail, and which algorithm and output contracts must that preserve?

For Astra:

1. Is Stage A's container (16 CPUs, 32 GiB, pools summing to 30 GiB) the
   right profile to add to the matrix runner, and should the
   graphframes-rs build go through the same rebuild script as the gate?
2. B2 changes the host's graph-utils service. Is a footer-derived receipt
   acceptable as the commit check, given the uncertain-write ownership
   rules in `staging.py`?
3. Which of B1 to B7 conflict with the checkpoint-partitioning proposal
   in `CLUSTER-PREPARATION.md` section 3, and should B1 wait for it?
4. Is C2's target (100 ms per distributed job at P = 16) realistic given
   what the stream diagnostics already show about task and stream setup?
5. Does the compact accumulator's replay schedule allow C4 before
   Stage A, so the baseline is taken once?

## 7. Not claimed

No timing in section 3 attributes a share of a round to a cause; Stage A
does that. No speedup is promised for any item in Stage B or C until its
paired run. The ratios in section 2 compare a 16-core in-process run on
his instance with a 32-core cluster-mode run on a shared host and are the
reason for Stage A's first step, not a result. Near-parity in local mode
says nothing yet about several hosts, which is Astra's cluster matrix and
the stream loss in [`STREAM-LOSS-STATUS.md`](STREAM-LOSS-STATUS.md).
