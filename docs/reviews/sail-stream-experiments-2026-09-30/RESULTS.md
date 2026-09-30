# Focused Sail stream and resource experiments

Recorded UTC: 2026-09-30T18:16:12.244600+00:00

Work in progress. This report separates the completed evidence from the compact
aggregation implementation and instrumented Linux replay still being qualified.
The prior review is [REVIEW.md](../sail-graphs-2026-09-30/REVIEW.md).

## Stream loss: one replay explained, earlier failures still open

The logged scale-24 Pecan BFS frontier replay hit its 100 GiB container limit.
Docker reports `OOMKilled=true`, and `memory.events` gained one `oom_kill`.
The subsequently retrieved Linux kernel log names the **same full Docker cgroup
ID** and records a Sail process killed by the memory cgroup. This is evidence
of an actual killed server process, beyond the earlier pooled memory reading.

Kernel PID 130101 was killed at monotonic 131805.374507, with 52,138,228 KiB
anonymous RSS. In the sampler window spanning that kill, container PID 174
(worker 2 in the server startup log) disappears; driver 56 and worker 1/PID173
remain for the subsequent samples. The namespace mapping was not recorded live,
so identifying the victim as worker 2 is an inference from the matching cgroup,
process disappearance, and memory measurements. Both workers were near 50 GiB.

The first logged transport error was `ConnectionReset` at 16:46:28 UTC, followed
by failed shuffle tasks and the familiar `error reading a body from connection`.
The recorded `NO_ERROR` GO_AWAY appears after session teardown began, so it is
not evidence that GO_AWAY initiated this failure. Memory exhaustion explains
this replay; it does **not** establish a common cause for earlier runs with no
OOM event. The new runtime and logging also differ from the historical frontier
run, so this is not an isolated before/after timing experiment.

Evidence: [cell orchestration](logging01/cell/orchestration.json),
[receipt](logging01/diagnostics/receipt.json),
[kernel attribution](logging01/kernel-oom-attribution.json),
[kernel excerpt](logging01/kernel-oom-excerpt.txt),
[server log](logging01/diagnostics/server.log), and
[process samples](logging01/diagnostics/memory-samples.jsonl).

The replay used runtime/native `ffcfbd5690e3f3681ef9ac18ba959bc231cf9f73`,
diagnostic harness `3a9028057c6c6c5034492845926fc4bc18f9626f`, two workers,
32 partitions and 96 GiB DataFusion pool **per process** inside a single 100 GiB
container. Those per-process limits do not bound their combined consumption.
The immutable input has 16,777,216 vertices and 268,435,456 undirected input
edge tuples. Settings and hashes are in the receipt; no dataset was regenerated.

The earlier h2 controls show why the outer error alone is insufficient:
ordinary cancellations do not consume the configured error-reset cap, and
keepalive expiry or abrupt peer loss can produce the same body-read message.
See [transport controls](transport-control/) and the preceding review. The
runtime diagnostic patch now retains bounded error sources, tonic status,
process, task and peer identity before error conversion. An additional actual
Tonic server control proved that `tonic::transport::server=debug` exposes
`http2 error: keep-alive timed out: operation timed out` without hyper tracing.
The same deliberately induced failure stays generic at `info`. This filter is
now included in the prepared no-OOM replay; the earlier hyper-only controls did
not exercise this Tonic logger. See [verified control](tonic-keepalive-control/receipt.json)
and [server log](tonic-keepalive-control/tonic-debug-keepalive-verified.log).

## Concrete aggregation allocation cost

The failing iteration's physical plan contains grouped
`min(struct(distance,hops,parent))`. DataFusion 55.1 has a specialized struct
group accumulator, but its state stores a separately copied singleton Arrow
struct per group. Its update builds scratch indexed by all resident groups,
constructs row slices, and compares via temporary Arrow structures. This is not
a claim that DataFusion falls back to one generic scalar accumulator per group.

The standalone probe compiles the unchanged pinned implementation and uses a
counting System allocator. At 100,000 groups it records:

| Quantity | Requested allocation bytes / count |
|---|---:|
| Three dense input columns retained | 2,401,010 bytes |
| Additional retained state after first grouped update | 204,831,488 bytes |
| Accumulator's own reported size | 68,800,000 bytes |
| First update allocations | 5,000,109 |
| Identical repeat allocations | 1,800,104 |
| Improving repeat allocations | 5,600,104 |
| Additional transient peak during final output | 84,852,196 bytes |

These are requested heap allocations on the local arm64 System allocator,
not RSS or a full-pipeline memory prediction. The dense input is a representation
control, not yet a production replacement. Exact output equality passed at
1,000, 10,000 and 100,000 groups. Prefix emission also leaves incorrect reported
remaining bytes; the current unordered failing plan's final emission is a
separate path, so that accounting defect is not assigned as its cause.

Evidence and reproduction: [probe source](min-struct-probe/src/main.rs),
[locked build receipt](min-struct-probe/build-receipt.json),
[100,000-group observations](min-struct-probe/100000-groups.jsonl).
The compact implementation is committed as
`56194b170155301ba91077f0ba3df31fe2c78b6b` on `work/compact-struct-min`, based on
the `2894a962` diagnostic/resource changes described below. At the same
100,000 groups it retains 4,194,304
requested heap bytes (48.8 times less), reports 4,194,730 bytes including fixed
schema/state storage, and performs 70 first-update allocations. An identical
repeat has an additional temporary peak of 504 requested bytes instead of
10,401,232; final output adds
2,400,000 bytes transiently instead of 84,852,196. All tested output values match.

The independent oracle exercises 64 seeds across 12 batches, child/root nulls,
filters, floating-point special values, signed IDs, prefix emission and state
merge. [STRUCT-MIN-ALLOCATION.md](STRUCT-MIN-ALLOCATION.md) records the method,
source fingerprints and every size. These are isolated requested allocations;
no whole-query or Linux RSS reduction is yet claimed.

The exact committed gate passed formatting, strict all-target Clippy for all
three changed crates, 149 execution, 328 function and 13 planner tests, and 46 Pecan
unit tests. The in-process worker codec test serializes and executes a physical
aggregate plan through the production worker decoding path and checks the
concrete compact accumulator. A separate Linux worker replay remains
pending. [Exact gate](compact-min-committed/receipt.json). Other `min` types
retain the original planner implementation and its metadata optimizations.

## Qualified implementation snapshot

The complete review snapshot is
[`b569e75de625885b3d919fa4196b2e0bed14c618`](https://github.com/querygraph/sail/commit/b569e75de625885b3d919fa4196b2e0bed14c618)
on the fork's `work/stream-review-integrated` branch. It combines the changes
below, compact MIN and BFS completion validation. Its exact detached gate passed
490 host Rust tests, 103 core tests, 49 native adapter tests and 46 Pecan unit
tests. [Integrated gate](integrated-review/final-receipt.json). This is a scoped
local review snapshot, not a Linux cluster or release verdict. The individual
work branches are also [verified on the fork](fork-branches.json). No upstream
pull request or main merge was made.


Repository `querygraph/sail`, branch `work/stream-performance-review`, exact
commit `2894a962076d3cc404dd72ec736ebeb9239901f6` contains the diagnostic logging
and three focused resource changes:

- Argentea SSSP terminal relay reuses the already validated labels. It avoids
  allocating candidates and scanning/copying every local vertex after Done.
  Producer completion and local EOF checks remain enforced. The all-owner
  statistics exchange remains quadratic in partition count.
- CSR construction reuses offsets as its fill cursor, saving eight admitted
  scratch bytes per local vertex on 64-bit targets. Validated dense owner-local
  IDs use direct lookup; irregular IDs retain checked binary search.
- Pecan/Grenada weighted traversal writes overflow detection with its winning
  candidate aggregation, avoiding a separate expansion action. A persisted
  marker adds overhead; the small paired experiment below did not show a speedup.

For four terminal relays and 65,536 vertices, the Done change reduces charged
work from 262,152 to 8 at one partition; at 32 partitions it changes 270,336 to
8,192. The optimized count is independent of vertex count for the tested fixed
partition counts. These are charged-work counts, not elapsed-time ratios.

For 65,536 vertices and 262,144 arcs, CSR construction removes one allocation
and reduces both total requested allocation bytes and admitted peak by 524,288
bytes. The measured requested live peak falls by 524,184 bytes for BFS and
524,256 bytes for SSSP. Dense weighted charged work changes 10,158,080 to
1,769,472; irregular
lookup work is unchanged. Raw input and completed CSR still overlap in memory.

Evidence: [Done relay](argentea-done/), [CSR](argentea-csr/),
[combined native candidate](argentea-combined-candidate/),
[exact integrated host gate](integration-committed/),
[exact native gates](integration-native-committed/).

The exact integrated gate passed formatting, strict sail-execution Clippy,
147 execution tests and 46 Pecan unit tests. The exact native core passed 101
release tests and the adapter passed all 48 release tests, including 42 Argentea
tests. Three additional adapter runs under ten load processes each passed all 48;
source fingerprints match the integrated source. A prior parser undercount and
fmt/control failures are retained alongside their resolutions, not removed.
This is a scoped gate, not a verdict on every Sail crate or a multi-host run.


## Argentea completion validation

A separate four-line core fix at
`193e2a9035428cc707bf09c0d20a16c421f353ba` validates BFS Reference/Push completion
totals against the producer's previously recorded frontier-edge count. The
baseline accepted all 12 malformed cases that suppressed the sole candidate and
rewrote completion counts to zero; the fix rejects them before publication.
Valid duplicate edges, self-loops and already-reached targets remain accepted.

Exact detached core formatting/Clippy and 103 release tests passed, as did 49
native adapter tests including 43 Argentea tests. A saturated run under ten load
processes passed both suites. Full native formatting has an unrelated existing
failure in unchanged files, reproduced byte-for-byte against 289; changed-file
formatting passed. [Evidence](bfs-completion/final-receipt.json).
This is a protocol integrity correction. No observed HTTP/2 stream loss has
been attributed to this malformed-record fault.

## BFS inbox allocation by traversal mode

Follow-up `c6126c27aae1b6262be3b1ecd6271008cc64fc69`, branch
`work/argentea-bfs-inbox`, is based on the integrated b569 snapshot. Eight
production lines avoid allocating candidate parents in Topology/Done and
allocate ghost membership only in Pull. Completion, sequence, mode and EOF
validation remain enforced.

At three partitions and 65,536 local vertices plus 65,536 ghosts, Done's
requested allocations fall from 1,117,679 to 3,567 bytes; its admitted peak
falls from 1,121,512 to 7,400 bytes. The optimized Done measurements match the
one-vertex and 1,024-vertex controls at fixed partition count. Push saves exactly
65,536 requested/admitted bytes; Pull is unchanged. All twelve matched cells,
including unchanged controls, are retained in [the counters](bfs-inbox/matched-counters.json).
These are allocation/admission measurements, not RSS or elapsed-time claims.
The old work-meter-only Done test had passed despite the unused allocations.

The exact detached core gate passed formatting, strict Clippy and 108 release
tests; the adapter passed 49 tests including 43 Argentea tests. Both suites also
passed under ten load processes, which were reaped. Four new allocation/headroom
cases fail on unchanged b569, while the wrong-mode protocol control passes.
The host's 490-test verdict remains scoped to b569; no new Linux/cluster verdict
is claimed for this native-only follow-up. [Receipt](bfs-inbox/final-receipt.json).

## Pecan production-path crosscheck and paired measurement

Both candidate production-path suites passed all 125 tests, once locally and
once with two process workers using the actual GraphUtils extension. The
baseline failed the same existing push-pull observer fixture in both modes
(115 passed, one failed each). Its observer consumed iteration-start events;
the candidate checks the intended iteration-end events and preserves assertions.
Those baseline suites remain `test_failure` in the evidence.

The immutable directed fixture has 16,384 vertices and 529,723 weighted edges,
seed42/source0. SSSP frontier uses an independent heap-Dijkstra reference and
parent-tree checks. All two warmups and eight measured cells passed. Both
labels use the same old runtime and native wheel; only the controller source
changes from ffcfbd569 to edc2c7c8. Every cell has a fresh container/server:
eight CPUs, 12 GiB hard memory, four partitions/threads, two workers, a 3 GiB
pool per process, and the same timeouts. The measured order is ABBA BAAB.

Morrobay is a **shared host**. Guest steal was zero in all these cells; that does
not establish an idle or dedicated physical host. The table reports each cell
relative to the measured baseline median in its own column, preserving slower
and larger-memory observations. Warmups are shown but excluded from medians.

| Cell | Outcome | Elapsed ratio | Execute PSS ratio | Execute cgroup ratio |
|---|---|---:|---:|---:|
| warmup-base | passed | 1.2559 | 0.9906 | 0.9956 |
| warmup-candidate | passed | 1.0735 | 1.0315 | 1.0547 |
| measured-1-base | passed | 0.9782 | 1.0004 | 0.9925 |
| measured-2-candidate | passed | 1.1776 | 1.0228 | 1.0606 |
| measured-3-candidate | passed | 1.1199 | 0.9620 | 0.9632 |
| measured-4-base | passed | 1.0072 | 0.9996 | 0.9948 |
| measured-5-candidate | passed | 1.0588 | 0.9686 | 0.9900 |
| measured-6-base | passed | 0.9928 | 1.0180 | 1.0403 |
| measured-7-base | passed | 1.0680 | 0.9980 | 1.0052 |
| measured-8-candidate | passed | 0.9852 | 0.9955 | 0.9908 |

The candidate median elapsed ratio is **1.0894** (slower), PSS is **0.9821**,
and sampled cgroup memory is **0.9904**. The narrow memory differences and
variable times do not establish an end-to-end improvement. Fixed marker/aggregate
cost on a small graph is a hypothesis, not a diagnosed explanation. A larger
isolated control is required before claiming benefit from this controller change.

All commands, package identities, raw cells and failures are retained in the
[plan](pecan-gate3/plan.json), [summary](pecan-gate3/summary.json), and per-cell
receipts beneath [pecan-gate3](pecan-gate3/). Raw times are measurement evidence;
the report makes only qualified shared-host ratio comparisons.


The subsequent Grenada crosscheck used the same fixture and limits, with one
baseline then one candidate and no additional warmups. Both passed independent
heap-Dijkstra/parent checks (distance tolerance 1e-12). Candidate/baseline ratios were 1.1359 elapsed,
1.0204 execute PSS, and1.0468 sampled cgroup memory. This is one shared-host
pair, not a statistical timing conclusion. It does not supply a performance
benefit for the fused controller on this fixture. [All outcomes](grenada-gate3/summary.json).

## Cluster preparation and remaining work

[The second Sem review response](SEM-REVIEW-2-RESPONSE.md) checks the proposed
local/process-cluster comparison and answers all five implementation questions.
An exact-arithmetic three-vertex control refutes equating ten-step delta
PageRank with ten-step power PageRank. It also identifies missing write-commit
semantics, nonpool memory and explicit job submission requirements. The source
review and control are separate from the pending Linux replay.

[CLUSTER-PREPARATION.md](CLUSTER-PREPARATION.md) traces the remaining single-node
and distributed costs and defines an eight-cell qualification matrix. It
separates placement with fixed total resources, strong scaling, and weak scaling.
The first barriers are simultaneous raw/CSR storage, active-round full-label
copies, quadratic control traffic, unrolled plan size, and Grenada's repeated
checkpoint write/read and lost partitioning contracts. The compact aggregation
addresses another per-worker memory cost before it is amplified across workers.

Near-linear cluster scaling has not been demonstrated. A homogeneous dedicated
cluster, per-worker hard limits, exact answers and partition/transport counters
are required for that claim. The available heterogeneous hosts can qualify
correctness and placement, not establish homogeneous scaling efficiency.
