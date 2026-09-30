# Sem's second review: relational graph measurements and a controlled investigation

Written 2026-09-30 for review by Astra and Sem.
Corrected UTC: 2026-09-30T18:12:32.343558+00:00. It records Sem's numbers
and his argument, sets ours beside them, separates what is measured from
what is read from source and what is still unmeasured, and proposes a
staged plan with proposed changes, controls, engineering budgets and
qualification boundaries. It builds on
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
`work/stream-performance-review` when it was written. The subsequent
[detailed review and answers](reviews/sail-stream-experiments-2026-09-30/SEM-REVIEW-2-RESPONSE.md)
identify contract, ownership, accounting and comparison limits. The original
line references in that review refer to the document hash in its
[source receipt](reviews/sail-stream-experiments-2026-09-30/sem-review2/source-receipt.json).
The stages below are proposals; this correction changes no code or defaults.

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
- On the input snapshot: "Pecan rewrites the inputs to Parquet? Why? The
  LDBC graphs are already available as Parquet files; I once asked the
  DuckLab people to make them, and they did." (Answered in Stage B7 and
  in Stage A's inputs.)
- On Banda: "I told you about this, and it matches what I saw when I
  benchmarked my tool: the conversion to CSR eats everything." (Stage F.)
- On the timer: "why on earth include validation in wall time." (Stage A,
  item 4.)

His results (`SemyonSinchenko/graphframes-rs`, branch
`new-benchmark-results` at `ba2fdd8f51fa7fafdca15012d2741f5f8d80c024`,
`benches/results`; c5d.4xlarge, 16 vCPUs, 32 GiB,
`--max-memory 30G --num-workers 16`; wall time / peak RSS / peak disk,
medians of 5):

| Graph | WCC | PageRank delta, cap 10, threshold 0.01 | Shortest paths |
|---|---|---|---|
| cit-Patents (3.77M vertices, 16.5M edges) | 4.71 s / 1.46 GiB / 0.23 GiB | 4.05 s / 1.06 GiB / 0.33 GiB | 0.90 s / 0.95 GiB |
| graph500-24 (8.87M reported vertices, 260M edges) | 33.3 s / 14.0 GiB / 6.2 GiB | 24.5 s / 5.0 GiB / 2.3 GiB | 6.7 s / 5.0 GiB |
| graph500-25 | 82.5 s / 18.6 GiB / 12.2 GiB | 62.3 s / 12.4 GiB / 8.6 GiB | 26.4 s / 12.4 GiB |
| graph500-26 | 213 s / 20.5 GiB / 28.8 GiB | 149 s / 14.3 GiB / 19.3 GiB | 72.9 s / 13.8 GiB |
| graph500-28 (121M vertices, 4.24G edges) | 1009 s / 20.2 GiB / 120 GiB | 912 s / 18.3 GiB / 91.4 GiB | 783 s / 17.6 GiB |

`--num-workers` is DataFusion's `target_partitions` in one process;
`--max-memory` is the spill pool. His WCC follows Bögeholz, Brand and
Todor (ICDE 2020), randomized contraction, with Parquet checkpoints
written and re-read between iterations. The receipts identify runtime source
`b4da56dabe20bba8e29563e06acc5179b2113ce3`; the checked algorithm, CLI and
monitor files are identical between those commits. Wall time covers subprocess
launch through exit, including input setup, algorithm work, final Parquet
writing and cleanup, with independent verification outside it. Peak RSS is
sampled process RSS. Peak disk is baseline-subtracted work-directory footprint
(checkpoints, spills, output and logs), not an isolated spill counter.
[Source and receipt audit](reviews/sail-stream-experiments-2026-09-30/SEM-REVIEW-2-RESPONSE.md).

## 2. Our recorded observations and comparison boundaries

Baseline `b87fb27ac`, Linux gate on shared Morrobay, 32 cores, 100 GiB
container, Sail in process-cluster mode (driver and two worker processes,
32 partitions). The following are retained campaign observations, not dedicated
host timing results or matched ratios against the external table.

| cit-Patents | Recorded observation |
|---|---|
| Pecan WCC, randomized contraction | 312 s, 19 rounds |
| Pecan WCC, min-label | 500 s, 20 rounds |
| Grenada WCC (the same controller over graph tables) | 397 and 566 s |
| Pecan PageRank power | 729 s, 20 iterations (setup 58 s, first iteration 23 s) |
| Pecan BFS | 164 s |
| Banda WCC | 30 to 39 s: staging 13.8, projection 14.4, kernel and output 1.4 to 10.6 |
| Banda PageRank power | 32 s: 13.8 + 14.6 + 3.3 |

The scale-24 campaign recorded completed relational push-pull BFS cells at
775 to 844 s. The two source-isolated cells recorded setup of 200 s (Pecan)
and 372 s (Grenada). Other cap failures, stream errors and OOM outcomes remain
in the [capacity record](reviews/gn-capacity-2026-09-29.md); a capped or failed
cell is not a completion-time result.

The cit-Patents ranking cells recorded 2.8 to 4.0 GiB sampled peak PSS; the
scale-24 relational reference/frontier cells recorded roughly 40 to 100 GiB.
These are different metrics from the external process RSS. Low sampled PSS
does not establish zero spill: record actual spill counters and pool pressure.

The hosts, resource envelopes, timing boundaries and graph manifests differ.
The external cit-Patents README lists 16,518,947 edges; ours lists 16,518,948.
Its Graph500-24 lists 8,870,942 vertices and 260,379,520 edges; ours includes
16,777,216 vertices and 268,435,456 input edge tuples. External shortest paths
use directed edges and a catalog-derived landmark; the scale-24 Sail cells use
an explicit hub and undirected edges, and certify parent/hops as well as
distance. External finite-step PageRank uses thresholded delta propagation
and final normalization; Pecan power redistributes dangling mass each step.
Equal iteration limits do not produce equal finite-step answers. These
observations motivate matched controls; they do not support cross-system
ratios or assignment of the difference to a cause.

## 3. Where the time goes

Each factor is marked **measured** (a receipt or a recorded run), **source**
(read from code, no timing attached) or **unmeasured**.

**F1. Every campaign cell ran Sail as a cluster; the external run is one
process.** *Measured on different hosts and inputs.* No cell of the original
campaign used `--mode local`. The declared-layout work (parity document,
section 13) recorded local-mode Pecan PageRank on 2M vertices and 16M edges,
four partitions, one process on Capitola, at 2.9 s per round and 12.5 s setup.
The gate's cluster-mode cit-Patents cell recorded 23 to 34 s per round and
58 s setup. The external PageRank observation is 4.05 s for its complete
capped-ten delta run. None of these pairs isolates execution mode or supplies
a matched per-iteration ratio.

**F2. A round has multiple data actions and control RPCs.** *Source; timing
observations are separate.* `StagingRun.materialize` (`staging.py:25-58`)
does a keyless `repartition(P)`, a Parquet write, a read back and schema check,
and an optional `count()` when the caller supplies `expected_rows`.
The unfused randomized WCC loop (`wcc_randomized.py:67-109`) has three writes,
one expected-row count on representatives, and two explicit counts for active
vertices and next edges: six explicit data actions per contraction round.
The initial `remaining` count is outside the loop. The fused loop has two
writes and two counts. Owned-run and schema/planning requests are additional;
these source counts do not establish physical server job/stage totals.

Min-label writes and counts the new labels, then joins the stored new and old
label tables to test for change; that comparison does not repeat the adjacency
expansion join. PageRank power has a dangling-mass action, a state write and
its expected-row count; a convergence action occurs only when tolerance is
specified. Fixed iterations with `tolerance=None` omit it. The last eleven
randomized WCC rounds, on a nearly empty contracted graph, recorded 4.6 to
5.7 s each. Extrapolating that floor over nineteen rounds gives about 90 s;
it is not a measured removable component or a proven constant for every graph.

**F3. Setup before the first round.** *Measured.* `_snapshot`
(`algorithms.py`) rewrites vertices and edges to Parquet, then runs five
validation actions (null ids, id uniqueness by group-by, null endpoints, two
anti-joins of every edge against the vertices) and a count: 28 s before
randomized WCC's first round, 39 s before min-label, 58 s before
PageRank. These boundaries include setup work specific to this controller.

**F4. Checkpoint reads do not carry the required key-layout contract.**
*Source and separate parity-work measurements.* A plain Parquet read does not
declare the partition/key mapping needed to reuse the prior distribution.
Count actual repartitions from each physical plan; not every join is proved to
shuffle both sides merely because that declaration is absent. The parity work
reports declared scans, host restatement and shuffle-free joins for its tested
plans. Its bucketed/sorted write observations were 11.6 to 12.5 s for
`partitionBy`, versus 1.5 to 5.8 s for plain writes of 16M rows. The paired
local-mode round observation was 4.95 s with layout versus 2.9 s without.
The ranges alone do not establish an 8–20 ratio. The cluster-mode comparison
remains unmeasured, and the declared-layout branch is separate from this
campaign's baseline.

**F5. Aggregation and estimation costs inside the engine.** *Measured in
isolation by Astra.* DataFusion 55.1's grouped `min(struct(...))`, which
Pecan's traversals use, retained about 2 KB per group and scanned scratch
for every resident group on every batch in the exact-source probe. At 100,000
groups the original/compact retained requested-heap ratio was 48.8, with the
compact implementation at `56194b170`. This is an isolated macOS allocator
measurement, not Linux process or whole-query memory. The compact path had
not yet been replayed on the gate; no whole-query gain is claimed.
[Probe scope and receipts](reviews/sail-stream-experiments-2026-09-30/STRUCT-MIN-ALLOCATION.md).
Separately, the aggregate's output is estimated at O(E) rows, which
drives join-side and broadcast choices (parity document, section 8).

**F6. Process pools do not bound total container memory.** *Replay and source
evidence.* The harness configured a 96 GiB pool in each of three processes
inside a 100 GiB container. In the logged scale-24 Pecan frontier replay, the
kernel OOM record matches that container and one worker disappears from the
sampler; the two workers had approached roughly 50 GiB each. This establishes
the OOM outcome for that replay, not the cause of earlier no-OOM stream losses.
It also does not establish that nothing spilled. Pool allocations, native
reservations, transport queues and other nonpool memory must be accounted for
separately; even pools summing below the limit do not guarantee a spill-only
outcome. [Evidence](reviews/sail-stream-experiments-2026-09-30/RESULTS.md).

**F7. Banda's recorded ingest is substantial.** *Measured.* On cit-Patents,
staging plus projection totals about 28 s. Kernel and output observations
range from 1.4 to 10.6 s for WCC; PageRank's corresponding phase is 3.3 s.
Keep phase and whole-run boundaries separate.

What is **unmeasured** is the split between F1 and F2 on one host: how
much of a 22 s round is the cross-process exchange and how much is the
controller's extra jobs. Stage A supplies controls but cannot by itself
identify each contribution. No factor above is assigned that causal share.
Shared-host times are observations, and a removed action is not a speedup until a paired
run shows one (the fused overflow change measured 1.09 times slower on
its first fixture).

## 4. The plan

Use explicit engineering objectives: exact admitted answers, disclosed
execution classes, bounded memory and disk, fewer redundant actions and bytes,
and measured scaling efficiency. Ratios against another implementation are
reported evidence only after contracts and boundaries match; they are not
predetermined acceptance outcomes. The absolute budgets below are proposed,
unqualified experiment thresholds, not demonstrated capability. Shared-host
measurements remain paired controls with steal and interference disclosed;
publish absolute timing results only on a qualified dedicated host.

### Stage A. Establish matched contracts and a local/process-cluster control

1. Pin graphframes-rs source, image, toolchain and binary in a separate detached
   build with its own target and receipt. Use the LDBC Graphalytics
   graphs in the Parquet form Sem had DuckLab produce as the shared inputs
   for both sides, once he says where they are; that removes the vertex
   and edge count differences of section 2 at the source. Define common input manifests,
   direction, duplicates/isolates, traversal source and requested outputs.
   For WCC, supplement the existing large-graph certificate with an independent
   reference partition or connectivity witness: equal labels along edges alone
   do not prove disconnected components were not merged.
2. Start with one small matched algorithm. A proposed profile is 16 CPUs,
   32 GiB container memory and local disk. Record actual threads, allocator,
   pool type and sizes, native reservations and nonpool headroom. The external
   CLI uses FairSpillPool; Sail's current harness uses a greedy pool. A proposed
   30 GiB total pool requires admission and pressure tests; it is not a safe
   memory guarantee. Different driver/worker pool sizes need harness support.
3. Preserve explicit local and process-cluster Pecan variants. For WCC, keep
   `randomized`, `randomized_fused` and `min_label` distinct. Before PageRank,
   choose identical finite-step recurrences or a common residual/error target.
   The current harness's positive tolerance and fixed-point certificate do not
   support fixed-ten power merely by setting the iteration cap to ten; add an
   explicit finite-step mode and oracle if that is the chosen contract.
4. Record algorithm-ready, result-exported and result-verified boundaries,
   setup and per-round time. Input validation is not algorithm work and is
   not in the external tool's timer (Sem, 2026-09-30: "why include
   validation in wall time"): report Pecan's snapshot and validation
   actions as their own phase and exclude them from the compared wall
   time, and give the library a trusted-input entry (Stage B7) so the
   compared run does not perform them at all. Excluding them changes no
   conclusion on cit-Patents (rounds alone are 284, 461 and 671 s), but
   it is the only boundary under which the numbers are comparable. Also
   record jobs/stages/tasks, plan bytes/planning time,
   exchanges, writer time, scalar actions, pool/spill counters and whole-process
   memory. Freeze warmup and ABBA order and retain every outcome. Estimate the
   campaign from the pilot: two inputs times three external, five local and
   five cluster configurations is already 26 configurations, or 104 cells at
   four measured samples each before warmups. No two-hour duration is promised.

| Observation | Supported conclusion | Next control |
|---|---|---|
| Matched local implementations differ | Their full measured paths differ; no single cause identified | Inspect plans, validation, aggregation, checkpoint and controller counters |
| Matched process-cluster and local cells differ | The combined execution-mode change matters in this profile | Separate scheduling, exchange, placement and serialization costs |
| Repeated client actions dominate measured round latency | Removing those particular actions is a candidate | A scoped Stage B ablation with the same answers and resources |

Maintain the independent multi-host placement, strong-scaling and weak-scaling
matrix in [CLUSTER-PREPARATION.md](reviews/sail-stream-experiments-2026-09-30/CLUSTER-PREPARATION.md).

### Stage B. Reduce redundant round work without weakening checkpoints

Each item is a separate opt-in candidate. Run the relevant unit tests plus
actual GraphUtils integration in local and process-cluster modes, lifecycle
failure controls and independent result checks. The existing 46 nonintegration
Pecan tests alone do not qualify checkpoint changes; the complete candidate
package's 125-test suite and its source/version boundary are recorded in the
[experiment results](reviews/sail-stream-experiments-2026-09-30/RESULTS.md).
Use matched paired controls; count actual jobs rather than inferring them from
Python actions. No public algorithm or execution-mode default changes here.

| # | Proposed experiment | Required boundary and evidence |
|---|---|---|
| B1 | Opt out of keyless `repartition(P)` before checkpoint writes | Inspect removed exchanges and resulting fanout/skew; preserve empty/schema/cancellation behavior. This can be tested now, but it does not declare key partitioning on the subsequent Parquet scan. |
| B2 | Add a bounded host write receipt with rows/files/bytes | Require confirmed writer completion and an exact immutable generation manifest with schema, file identity and ownership. Footer row totals can replace counts of that exact relation. Existing `gf.utils.v1` only supplies owned filesystem operations; a listing or valid footers cannot commit an uncertain write or authorize early cleanup. |
| B3 | Carry required round scalars with state | PageRank needs current dangling mass before the next rank; use an in-plan scalar or explicitly pipelined next-round mass with bootstrap, preserving the recurrence and reduction semantics. Commit state and scalar output consistently. Min-label may carry a nonnull changed flag; missing/unsupported footer statistics require a scan, never an assumption of convergence. Ordinary footers do not contain arbitrary floating-point sums. |
| B4 | Measure the existing `randomized_fused` variant explicitly | It adapts the PR 56 contraction approach while retaining original IDs. Existing integration tests compare each representative map. Keep the public default unchanged; qualify seed/duplicate/isolate behavior, `min_by` memory and certificates before any later default proposal. |
| B5 | Bounded contraction tail experiment below a measured threshold | Edge count alone does not bound plan growth or convergence. Preserve seeded representative choices, all reverse-expansion mappings, isolates, minimum original-ID labels, iteration caps and cancellation. Bound unrolling and retain a checkpoint fallback; eleven tail rounds are an observation, not guaranteed removable work. |
| B6 | Configurable checkpoint interval where plans remain bounded | Retain each immutable source generation until every lazy descendant has completed. Preserve uncertain-write ownership, cancellation and retry; cap plan bytes/depth and measure recomputation and peak memory. |
| B7 | Combine validation work and reuse a validated immutable input handle | Preserve BIGINT/schema, null/duplicate ID and both endpoint membership checks. A path or caller `trusted=True` assertion is not proof of immutability/validation. Pin the exact file generation and keep borrowed input ownership separate from run-owned outputs. Count physical scans/jobs; combining expressions does not prove one pass. |

B2 must test lost acknowledgments, partial/late writers, retries, cancellation,
empty stages, schema mismatch and file replacement. B3 companion output must
share a committed generation with state; it is not automatically client-only.
Total row count is an active-row count only for a relation containing exactly
those active rows. B5/B6 must not delete a checkpoint that a still-lazy plan
references; current deletion follows successful materialization.

Proposed pilot budgets on a qualified host are at most two data jobs per round,
round floor at most 1 s, setup at most 5 s, and cit-Patents randomized WCC at
most 30 s end to end. These are unqualified engineering budgets. A 60 s result
after B1–B4 would trigger review of measured remaining costs, not prove a
server loop necessary or license weakening semantics.

### Stage C. Measure distributed-job overhead and enforce resource envelopes

| # | Proposed experiment | Control and acceptance evidence |
|---|---|---|
| C1 | Compare local and process-cluster execution for one-host graph runs | Keep both explicit; decide any future default from qualified use cases and evidence. A local result does not qualify several hosts. |
| C2 | Measure planning/serialization, scheduling, stream creation and teardown at P = 4, 16, 32 with two workers | Force real distributed stages and an exchange that optimization cannot remove; 20 repetitions, cold/warm separation, p50/p95 and task counts. The proposed 100 ms warm-job budget at P = 16 is unqualified by current stream-fault controls. |
| C3 | Configure driver/worker pools with measured nonpool headroom inside a 32 GiB profile | Record pool reservations, native quotas, transport buffers, PSS, cgroup peak and spill separately. Preserve refusal, timeout and OOM as separate outcomes. Pool sums alone cannot ensure completion or spill instead of a kernel kill. |
| C4 | Qualify compact tuple MIN with the original controller, then probe `min_by` separately | Use the paired traversal control and certificates; report memory and time ratios, including regressions. The existing tuple MIN optimization does not cover fused WCC's `min_by`. |

The current harness copies the same pool setting to driver and workers; a
small driver pool plus larger workers requires configuration support, and a
native quota larger than the driver's pool can reject extension binding.

### Stage D. Stop re-shuffling the edges (the declared layout, write side)

The read side is finished (`work/declared-layout`). What remains is
section 14 of the parity document, reordered by what the campaign showed:

1. Measure the declared layout in cluster mode on the gate, where a
   shuffle may cross processes; whether removal is more valuable than in
   the local-mode experiment remains to be measured.
2. Report Sail's slow `partitionBy` write with the existing probe
   (`layout-exp/partitionby_probe.py`); the user files it upstream.
3. Profile the driver-side writer's FFI crossing if 2 stalls.

Proposed, unqualified engineering budget: retain only required exchanges and
keep the checkpoint-write overhead within 50% of its matched plain-write
control. Verify distribution declarations and physical exchanges explicitly.

### Stage E. Evaluate an explicit server-side iterative controller

Today Grenada is Pecan's controller entered through graph tables. A single
Spark Connect request per algorithm is an architectural candidate, not a
performance guarantee. Evaluate it only if component measurements justify the
scope. Two possible prototypes are:

- a driver-placed controller that explicitly submits each round through the
  job runner, with verified checkpoint layout and bounded retained state; or
- a graphframes-rs adapter with an explicit Sail submission path for each
  internal action. A DataFusion context alone does not turn library `collect`
  or write calls into Sail distributed jobs.

The current extension interface does not itself provide the session's
`JobService`. Prove controller placement, per-round submission and worker codec
support, cancellation, resource accounting and cleanup on one tiny algorithm,
then demonstrate remote worker execution. Local integration is not merely a
build question: algorithm and result contracts, storage ownership and execution
boundaries still need qualification. Argentea's job/operation-scoped state also
cannot simply be retained across new jobs without a continuation design.
Check the pinned library's current license/API before adapting it (Apache-2.0
was recorded at `b4da56d`).

### Stage F. Investigate Banda ingest

Sem's standing objection (his first review, `FABLE-ON-ASTRA.md` section 8,
and again on 2026-09-30: what he saw benchmarking his tool, "the conversion
to CSR eats everything") matches the cit-Patents receipts: about 28 s of
staging and projection around a 1.4 to 3.3 s kernel. Whether that is the
conversion or our conversion is the first control, before any change: a
native-only CSR build from the same `edges.parquet` (Int64 ids, no Sail
tables, no FFI crossing, no canonical sort), timed on the gate with the
same boundaries. Degree count, prefix sum and fill over 16.5M edges are
expected on the order of a second; that measured number is the floor, and
the gap between it and 28 s is what the candidates below have to close.
The crossover test of `FABLE-ON-ASTRA.md` section 8 stands: if the ingest
does not come under the relational path's first-round cost on graph500-24,
that is evidence for Sem's position and the decision guide says so.

Evaluate Int64 identity and dense u32 vertex targets from `FABLE-ON-ASTRA.md`
as separate candidates. Preserve usize/u64 arc offsets: a graph may fit u32
vertex IDs while symmetrized arc count exceeds u32. Use checked conversions
and explicit unsupported-size errors. Qualify `asStaged` order/determinism per
kernel before any default proposal; staging chooses edge order before the
algorithm runs. A proposed 5 s staging-plus-projection budget on cit-Patents is
unqualified and does not imply a cross-system end-to-end ratio.

### Order and cost

Finish the instrumented failure and compact-aggregation qualification, define
shared contracts and validators, then run Stage A's small pilot. Choose B/C
ablations from measured components while preserving the multi-host program.
B2 requires a host protocol change; B3 may require a writer/commit change too.
D and F remain separate candidates. Estimate duration from frozen dry plans,
resource admission and pilot outcomes; no day/hour estimate is established.

## 5. Proposed engineering budgets and qualification

These budgets are proposals rather than achieved results. All require pinned
inputs/protocols and complete answer validation; shared-host observations are
reported as matched controls, not published absolute results.

| Property | Proposed budget or required evidence |
|---|---|
| Controller work | At most two data jobs per round, measured from logs and plans |
| Small-round latency | At most 1 s on a qualified dedicated host |
| cit-Patents setup | At most 5 s with the same validation and snapshot contract |
| cit-Patents randomized WCC | At most 30 s end to end on the qualified pilot profile |
| Memory | Enforce total process/container admission with measured nonpool headroom; distinguish spill, refusal and OOM |
| Scaling | Fixed-resource placement plus strong/weak controls, with per-worker skew, bytes and driver/store load |
| Larger Graph500 admission | Manifest, arc-index bounds, memory model and disk-capacity check before launch |

The external Graph500-28 work-directory observation is 120 GiB, which includes
more than spill. Morrobay's volume had about 40 GiB free when this plan was
written; it cannot admit that footprint. Recheck live capacity before any
future run; no scale-28 launch is authorized by this document.

## 6. Questions

For Sem:

1. The pinned monitor/CLI includes input setup and final writing; can he
   confirm that the published runtime artifact matches those boundaries?
2. Which WCC variant and checkpoint interval produced the table, and is
   `prefer_smj` on?
3. His graph500-24 has 8.87M vertices and 260M edges, ours 16.8M vertices
   (8.86M reached from the hub) and 268M edge tuples. Is his input
   deduplicated and stripped of isolated vertices, and can he share the
   generator command so both sides read the same bytes? Better: where are
   the DuckLab Parquet conversions of the LDBC graphs he mentioned, so
   both sides read those?
4. Which graphframes-rs APIs support an explicit per-round Sail submission
   adapter with the agreed algorithm, output and ownership contracts?

For Astra:

1. Is Stage A's container (16 CPUs, 32 GiB, pools summing to 30 GiB) the
   appropriate proposed profile to qualify with nonpool headroom, and should the
   graphframes-rs build go through the same rebuild script as the gate?
2. B2 changes the host's graph-utils service. Is a footer-derived receipt
   acceptable as the commit check, given the uncertain-write ownership
   rules in `staging.py`?
3. Which of B1 to B7 conflict with the checkpoint-partitioning proposal
   in `CLUSTER-PREPARATION.md` section 3, and should B1 wait for it?
4. Is C2's target (100 ms per distributed job at P = 16) realistic given
   what the stream diagnostics already show about task and stream setup?
5. Does the compact accumulator's replay schedule allow C4 before
   Stage A, while retaining the instrumented original as a control?

## 7. Limits

No paired comparison was run for this document, and no Stage B/C speedup or
safe memory envelope is established. Section 2 preserves observations with
different contracts and hosts; it deliberately supplies no cross-system ratio.
Stage A can narrow mechanisms only with additional component controls. Local
qualification does not establish multi-host scaling. The earlier no-OOM stream
loss remains separate from the confirmed OOM replay and later diagnostic work
in [RESULTS.md](reviews/sail-stream-experiments-2026-09-30/RESULTS.md).
