# Pecan code and measurement harness — reading map for Sem

Pecan's implementation and benchmark harness live in the public
[`querygraph/sail`](https://github.com/querygraph/sail) fork. Grust holds the
review reports. Start with the DeltaStar loop and the timed adapter below.

## Which version to read

| Purpose | Source |
|---|---|
| Published Pecan review version | [`cab6bacc`, package directory](https://github.com/querygraph/sail/tree/cab6bacc0ad0d1fc8b3070e9e4267e99751909fe/examples/extensions/graph-algorithms/src/pyspark_pecan), published on `work/stream-review-followup` |
| Exact Python implementation and harness used for the 33.05 GiB scale-24 result | [`3a9028057`, package directory](https://github.com/querygraph/sail/tree/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/graph-algorithms/src/pyspark_pecan) |
| Sail runtime used for that result | [`56194b170`, compact grouped struct MIN](https://github.com/querygraph/sail/blob/56194b170155301ba91077f0ba3df31fe2c78b6b/crates/sail-function/src/aggregate/compact_struct_min.rs) |

These are intentionally separate pins. The measured Python version predates
the combined weighted-overflow aggregation and the optional checkpoint
repartition switch. The published review version includes those changes;
checkpoint repartition still defaults to enabled.

## Current Pecan code

All paths below are under `examples/extensions/graph-algorithms/src/pyspark_pecan/`
at `cab6bacc`.

| Read | File and responsibility |
|---|---|
| 1 | [`algorithms.py:292–308`](https://github.com/querygraph/sail/blob/cab6bacc0ad0d1fc8b3070e9e4267e99751909fe/examples/extensions/graph-algorithms/src/pyspark_pecan/algorithms.py#L292-L308): public `GraphAlgorithms.sssp` parameters and dispatch |
| 2 | [`traversal.py:9–41`](https://github.com/querygraph/sail/blob/cab6bacc0ad0d1fc8b3070e9e4267e99751909fe/examples/extensions/graph-algorithms/src/pyspark_pecan/traversal.py#L9-L41): input checks, undirected edge expansion, adjacency materialization |
| 3 | [`traversal_stepping.py`](https://github.com/querygraph/sail/blob/cab6bacc0ad0d1fc8b3070e9e4267e99751909fe/examples/extensions/graph-algorithms/src/pyspark_pecan/traversal_stepping.py): complete DeltaStar loop, active bucket, label updates and pending queue; 54 lines |
| 4 | [`traversal_relaxation.py`](https://github.com/querygraph/sail/blob/cab6bacc0ad0d1fc8b3070e9e4267e99751909fe/examples/extensions/graph-algorithms/src/pyspark_pecan/traversal_relaxation.py): `state.unionByName(candidates)`, grouped `min(struct(...))` and overflow reduction; 27 lines |
| 5 | [`staging.py:26–78`](https://github.com/querygraph/sail/blob/cab6bacc0ad0d1fc8b3070e9e4267e99751909fe/examples/extensions/graph-algorithms/src/pyspark_pecan/staging.py#L26-L78): checkpoint repartition, Parquet write/read and generation cleanup |

The DeltaStar method selects the lowest pending distance bucket and relaxes
all outgoing edges of its selected vertices. It is not classical light/heavy
delta-stepping. Each round combines candidates with all reached labels before
grouping, so a small active frontier does not make the entire plan small.

## Harness behind the reported measurement

These links all pin `3a9028057`, the actual measured controller, under
`examples/extensions/benchmarks/`.

| Part | Exact location |
|---|---|
| Parse parameters | [`graph_cell.py:371–421`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/graph_cell.py#L371-L421) |
| Launch Sail and connect the client | [`graph_cell.py:471–494`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/graph_cell.py#L471-L494), with process setup in [`runtime.py`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/runtime.py) |
| Read input, call algorithm, time it, write result | **[`traversal_cell.py:19–104`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/traversal_cell.py#L19-L104)** — 86 lines across the traversal adapters; Pecan's branch is **83–97** |
| Read container memory counters and process RSS/PSS | [`measurement.py:35–73`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/measurement.py#L35-L73); sampling starts at [`Sampler`, line 95](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/measurement.py#L95) |
| Validate after execution | [`graph_cell.py:494–513`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/graph_cell.py#L494-L513) calls [`traversal_cell.validate`](https://github.com/querygraph/sail/blob/3a9028057c6c6c5034492845926fc4bc18f9626f/examples/extensions/benchmarks/traversal_cell.py#L107-L119) |

The complete campaign harness is larger than 100 lines: it also owns input
identity checks, process lifecycle, correctness verification and failure
receipts. The timed traversal adapter above is the short path to inspect.

The timer starts at `traversal_cell.py:27`. Pecan is called at line 90;
`algorithm_ready_seconds` is recorded at line 91. The full result is written
at line 96; `end_to_end_seconds` is recorded at line 97. Server startup and
subsequent correctness verification are outside those timers. Pecan's internal
input snapshots, validation actions and intermediate checkpoints are inside.

## What the 33.05 GiB number measures

The run used one source, 16,777,216 vertices, 268,435,456 input edge tuples,
undirected execution, DeltaStar with delta 0.1, two worker processes and 32
partitions. It converged in 60 rounds.

**33.05 GiB is the whole-container lifetime cgroup peak**, including all
container processes, charged page cache and correctness verification.
Sampled process PSS peaked at **20.24 GiB during execution** and **26.64 GiB
during verification**. These are different measurement boundaries; none is a
measurement of the frontier alone, and their difference is not an allocation
breakdown. The host was shared, so recorded durations are diagnostic observations.

Evidence: [closed measurement review](https://github.com/querygraph/grust/blob/6bdd55748fa0e9233a051d52c6b0965786947676/docs/reviews/sail-stream-experiments-2026-09-30/logging03-closed-review/README.md)
and [completed physical-output check](https://github.com/querygraph/grust/blob/6bdd55748fa0e9233a051d52c6b0965786947676/docs/reviews/sail-stream-experiments-2026-09-30/COMPACT-REPLAY-AND-SSSP.md).

## UNION versus EXPLODE/UNNEST review targets

There are two distinct rewrites to test: direct/reverse edge expansion in
`traversal.py`, and reached-state/candidate merging in `traversal_relaxation.py`.
The latter is not a mechanical substitution of one operator: an equivalent
EXPLODE formulation may change joins and duplicate intermediate self labels.
Compare exact results and physical plans before comparing time and memory.
The focused comparison is being prepared; this document does not claim results
for it or infer Spark behavior from a DataFusion run.
