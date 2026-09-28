# Matching graphframes-rs: where its speed comes from, and how the fork gets it

Written 2026-09-28 against graphframes-rs `b4da56d` (DataFusion 55.0.1,
Apache-2.0), the `querygraph/sail` fork at `b87fb27ac` (DataFusion 55.1.0),
and Pecan as of `bd8ce9ae8`. Sem's claim, verbatim in translation: his
graphframes-rs, with direct access to DataFusion and the plan, comes out
about three times faster than the naive variant; and the trick is about 300
lines, half boilerplate, in `src/memory/hash_partitioned.rs`. He adds that
DataFusion 55 has some movement on range partitioning and co-partitioning,
still raw.

This document answers "make sure our direct DataFusion plan access is as
efficient as his" by naming the mechanisms, checking each against the fork,
and setting out the smallest change that reaches parity, with the
measurement that decides it. It corrects one claim in
[`FABLE-ON-ASTRA.md`](FABLE-ON-ASTRA.md) on the way.

## 1. What "naive" is, and what we have

The naive variant is a client-driven loop: Python builds one relational
round per iteration, the engine executes it, the state is written to
Parquet and read back, and nothing about the written files' layout is told
to the next round. Sem's PR 30 is that shape on plain PySpark. **Pecan is
that shape too.** Its `StagingRun.materialize` does
`frame.repartition(N).write.parquet(path)` then `spark.read.parquet(path)`
(`pyspark_pecan/staging.py`), and its PageRank round is three Spark Connect
jobs plus the write: the dangling-mass scalar, the convergence scalar, and
the state write (`algorithms.py`, `pagerank`). Grenada is Pecan with a
different entrance. The graph-table helpers (`GraphTables`: degrees,
triplets, walks) generate ordinary plans but have no loop. Nothing in the
fork runs an iterative relational algorithm inside the engine; the
graphframes-rs integration plan records that as "deferred server-side
option".

So the honest baseline is: our direct-plan path for iterative algorithms
does not exist yet, and our relational path is the naive one.

## 2. Where the three times comes from

Read from graphframes-rs `src/algorithm/pregel.rs`, `src/memory/hash_partitioned.rs`
and `src/memory/parquet_checkpointer.rs`.

1. **Declared co-partitioning and order on every checkpoint.** A checkpoint
   is written by a hand-built `RepartitionExec(Hash([key], N))` followed by
   a `SortExec` on the key with partitioning preserved, one Parquet file per
   bucket named `part-{i}` (`write_batches`; the comment explains why the
   plan is built by hand: the optimizer may drop a logical hash repartition
   over a single-partition source). It is read back through a
   `TableProvider` whose scan wraps the `ListingTable` source in a
   `DataSource` that declares `output_partitioning = Hash([key], N)` and an
   equivalence class with the key ordering (`HashPartitionedTable`,
   `HashPartitionedSource`). The comment at the scan says why the wrapper is
   needed: `ListingTable` groups files by statistics and would collapse N
   tiny buckets into one group, defeating both the partition count and the
   validated ordering. The edge relation is checkpointed once, pre-sorted
   by `src` (`pregel.rs`, `push_pre_sorted(..., EDGE_SRC)`), and each
   iteration's state by `id`.

   Consequence: the two joins of a round, state to edges on `src` and state
   to aggregated messages on `id`, satisfy `Distribution::HashPartitioned`
   on both sides and arrive sorted, so `EnforceDistribution` inserts no
   repartition and `EnforceSorting` no sort. With `prefer_smj` the join is a
   sort-merge join streaming two sorted files. The edge relation, the large
   side, is never shuffled again after the first checkpoint. That is the
   dominant per-round cost in the naive variant and it is gone.

2. **One engine round per iteration, in process.** The loop is Rust over
   `DataFrame`s in one `SessionContext`. There is no Connect round trip, no
   Python, and the convergence check is a `count` on the activity column of
   the state just written. Pecan pays three jobs per round plus the write;
   at cit-Patents scale (Sem's 14 s, 19 rounds) that fixed cost is a
   visible fraction. At Graph500-24 it is not.

3. **Messages as a filtered source-side join** (`skip_dest_state`): only
   participating sources are joined to edges, so the join input shrinks with
   the frontier. Pecan's power method already joins only `edges ⋈ rank`, so
   this is not a gap for reference PageRank; it is for frontier methods.

4. **Bounded disk.** Aggregated messages are checkpointed to cut the peak,
   old state is evicted after each round (`evict_all_but_latest_n(1)`), and
   everything is purged at the end. Pecan does the same through its own
   `GraphUtils.remove` (a zero-input extension relation the host owns), one
   stage per round.

   **Correction to `FABLE-ON-ASTRA.md`.** Section 3 and S5 say Pecan has no
   checkpoint purge. That is wrong: Pecan removes each round's stage through
   the host-owned `gf.utils.v1` relation (`crates/sail-session/src/extensions/graph_utils`).
   It is PR 30's checkpointer that has no purge. S5's "purge" item is
   therefore already done for Pecan and should be struck; the "layout" item
   is the one that matters, and it is this document.

Mechanism 1 is the three times. Mechanisms 2 to 4 are real but secondary.
Sem's pointer to the 300 lines is exactly mechanism 1.

## 3. What stands between the fork and mechanism 1

Two things, one on each side of the protocol.

**The write side is already expressible from the client.** Spark Connect
carries `repartition(N, key)` and `sortWithinPartitions(key)`, and Sail
plans them as `RepartitionExec(Hash)` and a per-partition sort. So Pecan's
`materialize` can write bucketed, sorted files today with no host change.
What must be checked, not assumed:

- that Sail's distributed Parquet writer emits exactly one file per input
  partition and that the file name carries the partition index (Sail uses
  DataFusion's `DataSinkExec` for Parquet; DataFusion's demuxer names files
  `{write_id}_{part_idx}.parquet` when no partition columns are given, and
  splits a partition into several files above
  `soft_max_rows_per_output_file`, 50 M rows by default). If the index is
  not recoverable from the name, the bucket must be written as a column or
  the write must go through a sink the fork controls.
- that the hash Sail's shuffle applies for `Partitioning::Hash` is the one
  the join's requirement is satisfied against. For the declaration to be
  *correct* it only has to be consistent between the two sides being
  joined, and both are written by the same shuffle; but it should be
  verified with a two-sided join on a fixture where a mismatch would show.

**The read side needs the declaring provider, and that is where the fork
has to act.** The 300 lines port directly: DataFusion 55.1.0 has the same
`DataSource`, `FileScanConfig`, `EquivalenceProperties` and
`with_file_sort_order` API as 55.0.1. The provider must be reachable from a
Connect client, and the natural place is a new relation verb in the Nutmeg
extension beside `stage`, `run`, `nodes`, `edges`: `checkpointed(path, key,
partitions)` returning the provider. That keeps it in `examples/extensions`,
with no upstream surface.

Then two host facts decide whether the declaration survives to execution:

- **Serialization to workers.** Sail's task codec serializes a
  `DataSourceExec` by recognizing its `DataSource` type
  (`crates/sail-execution/src/proto/codec.rs`: `FileScanConfig` with the
  Parquet, CSV, JSON, Arrow and Avro sources, `MemorySourceConfig`, and
  Sail's own remote source). A wrapping `DataSource` from an extension is
  not one of those, so a task containing it cannot be shipped. The
  declaration is needed only on the driver, where the physical optimizer
  and the job planner decide whether to insert a shuffle; the worker only
  needs the plain file scan. So the fork needs one of: the extension
  unwraps itself when the plan is serialized (the codec would have to ask
  it, a host change), or the codec learns a generic "declared layout" node
  that wraps any serializable scan with a partitioning and an ordering and
  is dropped on decode. The second is small, general, and is the kind of
  thing that later becomes a granular upstream PR. Sem's remark about
  DataFusion 55's co-partitioning work is the same idea at the engine
  level; if DataFusion grows a declared partitioning on `FileScanConfig`,
  the node disappears.
- **The job planner's left-join rule.** `ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows`
  (`crates/sail-execution/src/job_graph/planner.rs`) rewrites a
  `CollectLeft` hash join whose join type is Left, LeftAnti, LeftSemi,
  LeftMark or Full into a partitioned join with a fresh
  `RepartitionExec(Hash)` on both children, unconditionally: its
  `repartition` helper strips an existing repartition and adds a new one
  without checking whether the child already satisfies the distribution.
  Pecan's round ends with `vertices.join(messages, "id", "left")`, a Left
  join. The rule applies only when the optimizer chose `CollectLeft`,
  which it does when the build side is below the single-partition
  threshold; on a large graph the mode is already `Partitioned` and the
  rule does not fire. So the declaration works at scale and can be defeated
  on small fixtures, which is exactly where a functional test would look.
  The rule should check satisfaction before repartitioning. That is a
  five-line, verifiable change and another natural upstream PR.

## 4. The change, in order

Everything below is fork work. Nothing goes upstream until it is measured.

1. **Client write layout, no host change.** In `StagingRun.materialize`,
   write `frame.repartition(N, key).sortWithinPartitions(key)` when the
   caller names a key, and record the file inventory in the run. Verify
   the one-file-per-bucket and name-carries-index properties on Sail with a
   fixture; if they do not hold, write the bucket index as a column and
   have the read side group files by it.
2. **`checkpointed` relation in the Nutmeg extension.** Port
   `HashPartitionedTable` and `HashPartitionedSource` (attribution kept,
   Apache-2.0 both ways). Options: `path`, `key`, `partitions`. Refuse when
   the file count is not `partitions` or the key column is absent.
3. **Declared-layout node in the fork's codec.** A generic wrapper exec
   that carries `(partitioning, ordering)` over a serializable scan and
   encodes as its child. Test: a plan with two `checkpointed` scans and a
   join on the key produces a job graph with one stage and no shuffle
   (`EXPLAIN` on Sail shows no `ShuffleWriteExec` between them), and the
   same plan without the wrapper shows one.
4. **Fix the left-join rule** to skip children that already satisfy the
   hash distribution. Test with the small fixture where `CollectLeft` is
   chosen.
5. **Pecan rounds on the layout.** Edges checkpointed once by `src`; state
   by `id`; the round's two joins over `checkpointed` scans; the dangling
   and convergence scalars folded into the state write as columns of a
   one-row companion file so the round is one job plus the write.
6. **Measure.** cit-Patents and Graph500-24, one host, one memory
   envelope, same timer boundary (input handles to written result):
   graphframes-rs CLI at `b4da56d`; Pecan before; Pecan after 5; PR 30.
   Record wall time, peak PSS, bytes written, and the round count. The
   claim to test is that Pecan-after is within the run-to-run spread of
   graphframes-rs on Graph500-24, where per-round fixed cost is negligible,
   and that the remaining gap on cit-Patents is mechanism 2.
7. **Only then, the server-side loop.** If the gap on cit-Patents matters
   for the use case, the graphframes-rs integration plan (controller in the
   driver, one job per round through `JobRunner`) is the next step, with
   the controller-placement question answered first, as its review asked.

## 5. What would go upstream, distilled

Small, separately verifiable, in the order the measurements justify them:

- the job planner's left-join rule checking distribution satisfaction
  before inserting a repartition (item 4);
- a declared-layout node in the codec, or a `FileScanConfig` partitioning
  declaration if DataFusion 55's co-partitioning work provides one (item 3);
- `AggregateUDF` registration in the extension loader, which item 5 does
  not need but frontier methods with custom combiners will.

Nothing else in this document touches Sail's crates.

## 6. What is not claimed

No number here is a measurement of ours. Sem's three times is his
measurement on his hardware against his naive variant. Whether Pecan-after
matches graphframes-rs is decided by item 6 and reported with its envelope,
in the campaign format, with every outcome retained.

## 7. The endgame Sem describes, and what DataFusion already has

Sem's expected end state: once declared co-partitioning is in DataFusion and
Sail supports it, edges are written once and every read is a sort-merge join
in linear time. DataFusion 55 is part of the way there, and the direction is
range partitioning rather than hash:

- DataFusion 55.0.0 added native range partitioning, `Partitioning::Range`
  with a declared ordering and split points, so that pre-partitioned inputs
  can avoid repartitioning ([release post](https://datafusion.apache.org/blog/output/2026/08/25/datafusion-55.0.0/),
  [epic #25421](https://github.com/apache/datafusion/issues/25421)).
- `ListingTable::scan` now declares `Range` from the partition split points
  of a Hive-partitioned listing ([PR #25279](https://github.com/apache/datafusion/pull/25279));
  that PR also records why declaring `Hash` for such scans was wrong and
  dropped rows. This is the engine-level version of the declaring provider:
  the scan itself carries the layout, and no wrapper is needed.
- Open items that map one to one onto the fork's gaps: `JoinSelection`
  should check whether inputs already satisfy co-partitioning before
  choosing `CollectLeft` ([#25301](https://github.com/apache/datafusion/issues/25301)),
  the same defect as Sail's job-planner left-join rule; preserving
  partitioning through co-partitioned full outer joins
  ([#25436](https://github.com/apache/datafusion/issues/25436)); plan-time
  pruning for range partitioning ([#25437](https://github.com/apache/datafusion/issues/25437));
  and input-size checks before reusing join partitioning
  ([PR #25787](https://github.com/apache/datafusion/pull/25787)).
- Sail already plans `Partitioning::Range` into its shuffles
  (`job_graph/planner.rs`, and `ExplicitRepartitionExec` in
  `sail-physical-plan`), so a range-partitioned checkpoint is not foreign
  to it.

Two consequences for section 4. First, write the checkpoints
range-partitioned by key rather than hash-partitioned: sort by the key,
split into N buckets by split points, one file per bucket, and record the
split points beside the files. Range gives the same co-partitioned merge
join, is what DataFusion's scans are learning to declare natively, and is
what Sail's planner already shuffles on, so the interim wrapper of item 3
becomes the smallest possible shim and disappears when Sail moves to a
DataFusion where the listing declares it. Second, the messages side of the
round, which comes out of a hash aggregate, would then need one range
repartition per round; that is the small side, and it is the trade the
engine direction implies. graphframes-rs's hash layout stays the reference
for the measurement in item 6 either way.

Sources: [DataFusion 55.0.0 release post](https://datafusion.apache.org/blog/output/2026/08/25/datafusion-55.0.0/),
[epic #25421](https://github.com/apache/datafusion/issues/25421),
[PR #25279](https://github.com/apache/datafusion/pull/25279),
[#25301](https://github.com/apache/datafusion/issues/25301),
[#25436](https://github.com/apache/datafusion/issues/25436),
[#25437](https://github.com/apache/datafusion/issues/25437),
[PR #25787](https://github.com/apache/datafusion/pull/25787).

## 8. A hash-join trap Sem names, checked against DataFusion 55.1

Sem's warning: in the round's join of edges to vertex state, followed by a
group-by and the join back to vertices, the group-by output really has
O(|V|) rows, but DataFusion sizes the build side conservatively and tries to
allocate O(|E|), which under a memory limit ends in an out-of-memory
failure. What the DataFusion 55.1.0 source the fork compiles against
actually does:

- **The estimate is O(|E|).** An aggregate's output row estimate falls
  back to its input's row count, made inexact, when the distinct count of
  the group key is unknown (`datafusion-physical-plan-55.1.0/src/aggregates/mod.rs`,
  `estimate_num_rows`), and its byte estimate is that row count times the
  row width. A group-by over the E-row message stream is therefore
  estimated at E rows unless something upstream knows the key's
  cardinality.
- **The estimate drives the plan, not the runtime reservation.**
  `JoinSelection` picks the build side and the `CollectLeft` broadcast mode
  from `total_byte_size`, then `num_rows` (`datafusion-physical-optimizer-55.1.0/src/join_selection.rs`,
  `should_swap_join_order`, the `hash_join_single_partition_threshold`
  checks). At execution the build side reserves by the actual bytes of each
  collected batch (`hash_join/exec.rs`, the `try_fold` over the build
  stream) and sizes its table by the actual row count; the dense
  "perfect hash" array map reserves by the key range, which for dense
  integer ids is O(|V|). So in 55.1 the O(|E|) figure decides which side is
  built and whether it is broadcast to every partition; a wrong choice
  there is memory amplified by the partition count, which is the failure
  Sem describes, and older versions reserved more eagerly.
- **The mitigation is the one graphframes-rs already applies.** It
  checkpoints the aggregated messages before the join back to vertices
  (`pregel.rs`, "spill aggregated to disk to reduce memory peak"), so the
  join sees a file with exact statistics of O(|V|) rows, not an estimate.
  Under a pool, `prefer_smj` avoids the build side altogether. Pecan's
  power method joins the un-materialized group-by directly
  (`vertices.join(message, "id", "left")`), so item 5 of section 4 must
  materialize the aggregated messages first, as a bucketed checkpoint on
  `id`, which also feeds the co-partitioned join. DataFusion's own fix is
  [#25301](https://github.com/apache/datafusion/issues/25301): check
  co-partitioning before choosing `CollectLeft`.

## 9. The GraphX-style design Sem sketches is Argentea

Sem's further sketch: keep a routing hash table in memory and use the
triplet memory model, as default GraphX does, but keep the triplets on disk
by partition and route every vertex-state update directly to the partitions
that need it. From the couch, two to three times faster again than
graphframes-rs; much harder to code; needs the graph-partitioning
literature, although 2D partitioning already carries well. He adds that it
is not a Spark Connect extension's level, because it means going into the
tokio workers and doing something like `mapPartitions`; that it is hard to
pull in without breaking the Sail runtime, being a hack over the plan; and
that range partitioning or bucketing is the proper way, compared with
declaring partitioning in the plan by hand.

That design exists in the fork. It is Argentea, on
`work/extensions-traversal-bench`: native adjacency partitions held by
Sail workers for the lifetime of one job, vertex-state updates carried as
typed messages through Sail's own shuffles to the owning partition, rounds
unrolled as native stages, and the placement of native stages pinned
through Sail's slot groups. Sem's prediction of what it costs is accurate,
and the integration document records the price: worker-side decoding of
native relations, native admission on workers, job-owned native state, one
attempt for regions holding native state, and placement validation. Those
are the focused host hooks it needed; they are exactly the "going into the
workers" he expects, and they are the reason it is qualified only on tiny
fixtures so far and only within a bounded number of rounds per job. The
partitioning question he raises is open there too: Argentea partitions by
Sail's ordinary hash shuffle, with no 1D or 2D vertex-cut strategy yet.
What it has that his sketch lacks is the memory accounting through the
host pool and the failure and cancellation qualification.

So the three layers line up as the plan already has them. The relational
loop with declared layout (this document's sections 3 to 5) is the
portable path and the one to make honest against graphframes-rs first. The
range-partitioned, bucketed form of it (section 7) is the proper version
Sem prefers, and it is where DataFusion is heading. Argentea is the
GraphX-style layer above both, and its next question, continuation across
jobs and a session-scoped native state, is S6 of `FABLE-ON-ASTRA.md`.
Whether it is two to three times faster than a co-partitioned relational
round is a measurement Argentea cannot give yet, because it has no
capacity evidence; item 6's matrix is the place it enters once it has.

## 10. Baseline observed on Sail, 2026-09-28

Run locally on Capitola against the delivered x86_64 binary
`target/extensions-datafusion-final/mac-x86-de8e67098/sail` in local mode
with four partitions, 100,000 vertices and 800,000 synthetic edges, client
writes `repartition(4, key).sortWithinPartitions(key).write.parquet`.
Script: session scratchpad `layout-exp/baseline.py`. What the plans show:

- **Sail writes one file per partition and the name carries the index**:
  `{token}_{i}.zst.parquet`, `i` in 0..3. Section 3's first question is
  answered without a host change.
- **The Parquet scan regroups files by byte ranges.** The state files came
  back as four groups of one file each, but the edge files came back as
  four groups made of byte ranges that straddle files
  (`_0:0..1315012, _1:0..2136` in one group). The buckets are destroyed at
  scan time, before any declaration could matter. This is the
  `ListingTable` behavior graphframes-rs's provider exists to override, and
  it means the read side must build its own file groups, one file per
  group, in index order.
- **Every join repartitions both sides**: `RepartitionExec(Hash([src],4))`
  under the edge scan and `RepartitionExec(Hash([id],4))` under the state
  scan for the message join, again for the aggregate, again for the
  vertices join. Three hash shuffles per round of what is 800,000 rows
  here and 260 M rows on Graph500-24.
- **Sem's estimate, live**: the aggregate over the 800,000-row message
  stream carries `Rows=Inexact(800000)` into the join back to vertices,
  whose true output is 100,000 rows. The optimizer chose `CollectLeft` with
  the 100,000-row state as the build side, which is the harmless
  orientation here; in Sail's distributed planner that same
  `CollectLeft` + Left join is what triggers the unconditional repartition
  rule of section 3.

The round took 32 ms at this size; the point of the run is the plan shape,
not the time.
