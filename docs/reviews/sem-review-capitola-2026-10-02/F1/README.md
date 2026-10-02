# F1, first control: where Banda's ingest time goes

Item F1 of [`SEM-REVIEW-2.md`](../../../SEM-REVIEW-2.md), section 9. Measured
on Capitola on 2026-10-02 (Apple M1 Max, release host and a release Nutmeg
wheel, `querygraph/sail` `0d1ef2ca3`, local mode, 10 threads, `asStaged`).

## The question

F0 put the floor of a CSR build from Parquet at 0.4 s for cit-Patents and
8 s for graph500-24. Banda's staging and projection took about 28 s and 650 s
on the gate. Before changing anything: how much of that is the gate, and
where inside Banda is the rest.

## Result: Banda end to end, Parquet in, Parquet out

One run per graph. Seconds. The ids are cast to strings before staging, as
the harness does.

| Phase | cit-Patents | graph500-24 |
|---|---|---|
| Server start and session | 0.4 | 0.4 |
| Read Parquet and stage | 0.4 | 1.9 |
| First WCC call, written to Parquet | 7.6 | 136.8 |
| of which the projection build | 7.3 | 135.7 |
| Second WCC call, written to Parquet | 0.26 | 1.1 |
| PageRank, 10 steps, written to Parquet | 0.40 | 4.1 |
| **One call, launch to exit** | **8.4** | **139** |
| Native memory at the peak | 3.3 GB | 41 GB |
| Admitted for one projection | 1.2 GB | 14.4 GB |

The run also built a second, undirected projection through
`projectionStats`, which WCC does not use (9.7 s and 157 s). That was the
script's mistake. It is left out of the one-call total and is the reason the
peak memory holds two projections.

## The three paths on one machine

Launch to exit for one WCC over the same LDBC files, on Capitola.

| Path | cit-Patents | graph500-24 |
|---|---|---|
| graphframes-rs | 3.7 s | 27.7 s |
| Pecan | 5.0 s | 30.4 s |
| Banda, first call | 8.4 s | 139 s |
| Banda, each further call | 0.26 s | 1.1 s |
| CSR floor (F0, 4 threads) | 0.4 s | 7.7 s |

- For one call the relational path is faster than Banda at both sizes: 1.7
  times at cit-Patents and 4.6 times at graph500-24.
- Banda's further calls are 20 to 30 times faster than a relational call.
  At graph500-24 the projection is repaid at the fifth call.
- Sem's reference, icebug, builds this CSR in 179 to 186 s on 4 cores. Banda
  builds it in 136 s here, on one core of a faster machine. It is in that
  class. It is 18 times the floor.

## How much was the gate

| | Gate | Capitola | Gate over Capitola |
|---|---|---|---|
| Banda ingest, cit-Patents | about 28 s | 7.7 s | 3.6 |
| Banda ingest, graph500-24 | about 650 s | 138 s | 4.7 |
| graphframes-rs WCC, cit-Patents | 13.75 s | 3.66 s | 3.8 |
| Pecan WCC, cit-Patents | 51.8 s | 5.00 s | 10.4 |

Banda slows down on the gate about as much as graphframes-rs does. Its ingest
time is its own. Pecan is the one path the gate slows by much more (A5).

## Where the projection's time goes

The projection is `GraphProjection::from_arrow_batches` in
`crates/grust-algorithms/src/arrow_input.rs`, followed by `from_buffers` in
`projection.rs`. It runs on one thread. A stack sample of the server during
the cit-Patents build
([`projection-sample-top-of-stack.txt`](projection-sample-top-of-stack.txt),
5,700 samples):

| Where | Share |
|---|---|
| `from_arrow_batches` itself: the string-keyed map lookups for both endpoints of every edge | 33% |
| Hashing the strings (SipHash, the standard library's default) and map probes | about 10% |
| `ExecutionContext::admit_work` and `charge_work`: work accounting, called once per row | 18% |
| `arrow_input::required`: the null check and value fetch, once per string | 14% |
| `Adjacency::put` and `build`: the CSR fill | 10% |
| `from_buffers`: a second map from node id to index, with cloned strings | 5% |
| Pushing node ids and edges | 3% |

What the code does, read from the source:

- Every vertex id is a string. A `HashMap<&str, Option<usize>>` with the
  default hasher maps it to an index.
- Every edge costs two string hashes and lookups, one work-accounting call,
  one label check, and a 40-byte `ProjectionEdge` (source, target, ordinal,
  optional edge id).
- Every node id is copied into an owned string, and copied again into a
  second map kept with the projection.
- Nothing is parallel.

That is the conversion Sem described: everything to strings, then a string
map.

## What would close it

In order of size. None is done here.

1. **An Int64 identity path (S1).** Accept BIGINT `node_id`, `source` and
   `target`. Map ids with a direct table or an integer hash. No string
   hashing, no per-node allocation, no second string map. This changes
   `GraphProjection`'s identity type, which today is Grust's string `NodeId`
   everywhere, including in the result columns. It is the large item.
2. **Compact edges and a parallel fill (S3).** The floor's build is a
   parallel count, prefix sum and fill over dense u32 targets under a checked
   bound. Banda keeps 40 bytes an edge before the adjacency exists.
3. **Accounting per batch, not per row.** 18% of the build is the work
   counter. Charging once per batch keeps the budget and cancellation checks.
   This is small, local and independent of the identity question.
4. **Null checks per batch.** 14%. Check `null_count` once, then read values
   directly.

Items 3 and 4 together might take a third off the build without touching the
identity model. They do not change its order of magnitude. Items 1 and 2 are
a change to `grust-algorithms`, which is a published crate pinned by the
extension at `=0.23.0`, so they need a Grust release and a re-vendoring.

## Limits

- One run per graph, a laptop, a warm cache. The gate numbers are from the
  September campaign and Stage A, on other builds.
- The sample is of cit-Patents only.
- WCC only for the three-path table. BFS and PageRank were not compared
  across paths here.
- Banda's WCC result was written but not checked against an oracle in this
  script. The row counts match the vertex counts.

## Files

`banda_phases.py`, `banda-cit-asStaged.json`, `banda-g500-asStaged.json`
(phases, stage receipt, projection builds, native status).
