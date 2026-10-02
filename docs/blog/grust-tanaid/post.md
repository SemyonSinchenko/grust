# Grust Tanaid: integer identity, and a projection that keeps eight bytes an edge

Grust gives Rust applications one property-graph API across memory, embedded databases, SQL systems and remote graph services. Nodes, edges, typed identities, traversals, mutations, schema and graph algorithms are written once against that API; Memory, Sail/Spark, PostgreSQL, pgGraph, PostgreSQL SQL/PGQ, Turso, SurrealDB, FalkorDB, LanceDB and CocoIndex sit behind it, and each adapter states which operations it pushes down, runs natively, answers through the portable reference, or does not support. `grust-cypher` is the portable GQL/Cypher layer over the same model. Langoustine 0.23.0 was about what sweeping a graph costs. Tanaid 0.24.0 is about what it costs to get the graph into a form a kernel can sweep, and to keep it there.

See the [repository and API guide](https://github.com/querygraph/grust), the [Grust book](https://firstpair.org/read/grust/), the [GQL profile statement](https://github.com/querygraph/grust/blob/main/docs/GQL_PROFILE_STATEMENT.md) and the full [`CHANGELOG.md`](https://github.com/querygraph/grust/blob/main/CHANGELOG.md).

Nothing in this release changes an answer. No kernel was touched beyond reading edges from columns where it read them from records. What changed is the projection: how it is built from Arrow batches, what it holds beside its adjacency, and when it builds the one structure most kernels never read.

## Where the time was

Every kernel in `grust-algorithms` runs over a `GraphProjection`: a packed adjacency, the node identities, and a table of the original edges. An embedder that stages a graph and runs many kernels builds the projection once, so the build is the price of the first answer. A review of one such embedder found the build to be most of that price. On a graph of 260 million edges it took about two minutes, and the kernel that followed took about a second.

A stack sample of the build said why, and none of it was the adjacency. Grust's node identity is a string, so the Arrow path hashed two strings for every edge to find its endpoints. It charged the work budget one unit a row through a shared counter, row by row. It checked every string for null. It kept forty bytes for every edge in a table kernels index to recover an edge's ordinal. And it built a second hash map from every node id to its row, which only a kernel that takes a source by id ever reads. The parallel count and fill that Langoustine gave the adjacency were a tenth of the time, and ran on one thread anyway because the embedder had not asked for workers.

## Integer identity, at the door

`GraphProjection::from_arrow_batches` now takes `node_id`, `source` and `target` as Utf8, as before, or all three as Int64. With integer ids an endpoint is resolved to its node row through a direct table when the ids span at most sixteen slots a node, and by binary search over the sorted ids when they do not. Neither hashes anything. That matters beyond speed: the lookup does not depend on a hasher's seed, and no choice of ids can make it degenerate.

The projection's external identity did not change type. A node built from the id `42` is the node `"42"`, in `node_ids()`, in every result column, and as a kernel's source. That is a string a node, where the old path paid two string hashes an edge, and it means a graph handed over as integers and the same graph handed over as their decimal text are the same projection. The test for the new path holds it to exactly that: the Utf8 path is the oracle, and the two must agree on node order and identity, on every edge's endpoints, ordinal and id, on the weights a shortest-path kernel reads, on the recorded selection, and on the first error for a bad input.

With no label selection every edge is kept and its position is its ordinal, so the edge table is filled in place, in parallel when the execution asked for workers. A label selection drops edges, so that case fills in order on one thread.

**The first bad edge is the same bad edge at every width.** A parallel fill that reports whichever chunk failed first would name a different edge on a different machine. Each chunk instead stops at its own first failure and records it; the smallest ordinal wins. A missing endpoint or a rejected weight is named identically with no concurrency, with one worker and with sixteen, and identically to the Utf8 path.

## Eight bytes an edge

Beside the adjacency a projection keeps its edges, indexed by slot, so a kernel that returns a path or a spanning tree can say which original edges it used. That table was a `ProjectionEdge` an edge: source, target and ordinal as three `usize`, and an optional external id. Forty bytes, of which most projections need eight.

It is now columns. Two four-byte endpoints an edge, the width the adjacency already stores a target in. An ordinal column only when some edge's ordinal is not its slot, which a graph handed over whole never has. An id column only when some edge has an external id. And inside the adjacency, an arc's original-edge slot is four bytes instead of eight: an edge has at least one arc, so a slot is below the arc count the row offsets already bound, and the `u32::MAX` ceilings Langoustine introduced cover it with no new refusal.

The arithmetic, from the structures:

| | before | now |
| --- | ---: | ---: |
| edge table, per edge, no ids, ordinals are slots | 40 bytes | 8 bytes |
| edge table, per edge, with ordinals | 40 bytes | 16 bytes |
| outgoing arc, unweighted (target + original-edge slot) | 12 bytes | 8 bytes |
| outgoing arc, weighted (+ `f64` weight) | 20 bytes | 16 bytes |

This is the release's one breaking change. `GraphProjection::edges()` returned `&[ProjectionEdge]` and now returns `Edges`, a view with `len`, `get(slot)`, `ordinal(slot)` and `iter()` that hands out `EdgeRef { source, target, ordinal, id }` by value. `ProjectionEdge` is still what `from_topology` takes.

## A map built by whoever needs it

A kernel that takes a source by id, a breadth-first search say, looks the id up in a hash map of every node id. The map was built with every projection. On the 260-million-edge graph that was 8.9 million string inserts, about a third of what remained of the build once edges were columns, for a map PageRank and weak components never open.

The adapters that map ids to rows themselves have already refused a duplicate id by the time the projection is assembled, so they now leave the map to the first lookup. `from_topology` still builds it at once, because that is where its duplicate check lives.

## What a budget means did not move

Cooperative budgets are how a shared caller is given an algorithm without being given the machine, and a cheaper build that quietly charged less would not be cheaper. Three things were held fixed.

- **A completed build charges the identical total.** The Utf8 path now makes one charge a batch instead of one a row, and only where the row charge is the loop's only charge and the budget covers the batch. `tests/projection_budget.rs` pins six projections to the work units the `v0.23.0` tag charged for them, at no concurrency and at four workers.
- **A budget that runs out, runs out where it did.** One that cannot cover a batch takes the row-by-row path, so it is refused at the same row with the same work counted, and an error in an earlier row still comes first.
- **The deferred map was paid for at the build.** The build charges one unit a node and retains the map's bytes whether or not it builds the map, so the first lookup charges nothing and admits nothing, and a projection's cost does not depend on which kernel happens to run first. The lookup polls, so a cancelled query does not finish it.

Admitted bytes went one way only. The same six projections must not admit more than they did in 0.23.0, and they admit between a sixth and a half less.

## What the measurements say, and where they stop

`benchmarks/projection-ingest` times `from_arrow_batches` over the LDBC Graphalytics Parquet files and commits every run. **The host is one Apple M1 Max laptop. These figures show the shape of the change; they are not results to quote**, and nothing here was measured on a dedicated host or on Linux. Median of three runs unless marked.

Seconds to build the projection:

| Graph, orientation | 0.23.0, Utf8 ids | 0.24.0, Utf8 ids | 0.24.0, Int64 ids, no concurrency | Int64 ids, 4 workers | Int64 ids, 8 workers |
| --- | ---: | ---: | ---: | ---: | ---: |
| cit-Patents, 16.5M edges, outgoing | 8.09 | 6.54 | 0.86 | 0.42 | 0.34 |
| cit-Patents, undirected | 8.95 | 7.41 | 2.01 | 0.63 | 0.53 |
| graph500-24, 260M edges, outgoing | 119.0 (one run) | 106.3 (one run) | 14.84 | 4.19 | 3.10 |
| graph500-24, undirected | 144.1 (one run) | 129.7 (one run) | 37.43 | 6.65 | 4.70 |

Bytes the projection's execution admits, which depend on neither the id type nor the worker count:

| Graph, orientation | 0.23.0 | 0.24.0 |
| --- | ---: | ---: |
| cit-Patents, outgoing | 1.13 GiB | 0.57 GiB |
| cit-Patents, undirected | 1.31 GiB | 0.70 GiB |
| graph500-24, outgoing | 13.38 GiB | 4.65 GiB |
| graph500-24, undirected | 16.29 GiB | 6.59 GiB |

Every run prints a checksum: node, edge and arc counts, the largest out-degree, and the number of weak components. It is the same in every row.

Two things in the first table are worth reading closely. The Utf8 column moved by a tenth to a fifth, which is the batch charges and nothing else; the two string hashes an edge are still there, and that is what string identity costs. And the undirected graph500-24 build with no concurrency asked for takes twice as long as the same build with one worker: an execution that says nothing about threads runs the sequential adjacency code, and one that asks for a single worker runs the parallel implementation on one thread, which is the better algorithm as well as the threaded one. That distinction is Langoustine's, kept so a benchmark can separate the two; an embedder that wants the faster build should ask for workers.

## What is not here

**Results are still text.** A graph built from Int64 ids returns Utf8 `nodeId` columns holding decimal text. Integer identity is accepted at the door; it is not yet a second identity type through the kernels and their outputs.

**Only Int64.** Other integer widths, and unsigned ids above `i64::MAX`, must be cast by the caller.

**No sparse-id timing.** Both measured graphs have compact ids and take the direct table. The sorted lookup is tested for correctness against the Utf8 oracle and was not timed.

**No Cypher surface changed, and no backend was re-qualified.** These kernels run over a projection built from a snapshot, so no backend planner is involved.

**Two tests fail without the `parallel` feature**, as they did in 0.23.0: each presupposes a parallel kernel. The release gate runs with all features.

## Upgrading

Tanaid is a lockstep release: move every direct Grust crate dependency to 0.24.0 together.

- `GraphProjection::edges()` returns `Edges` rather than `&[ProjectionEdge]`. `graph.edges()[slot].ordinal` becomes `graph.edges().ordinal(slot)`; `graph.edges()[slot]` becomes `graph.edges().get(slot)`, an `EdgeRef` by value whose `id` is `Option<&EdgeId>`; a `for edge in graph.edges()` loop compiles as it was.
- `projectionStats` and `estimateCsr` report smaller byte figures for the same graph, because an outgoing arc is four bytes narrower. A test that pinned the old numbers is pinning the old representation.
- To use integer identity, hand `from_arrow_batches` Int64 `node_id`, `source` and `target` columns instead of casting them to text, and give the execution a concurrency with `ExecutionContext::with_concurrency` if the build should use more than one thread.

Everything else is additive.
