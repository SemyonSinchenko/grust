# Graph Nuts master plan

Graph Nuts is the umbrella for graph algorithms executed through Sail and
validated with independent graph benchmarks. It covers the algorithm kernels,
Sail extension wiring, large-graph qualification, and the published evidence.
The name describes the benchmark family; it does not create a new execution
engine or imply a combined leaderboard.

## Objectives

1. Provide correct PageRank, WCC, BFS and shortest-path implementations with
   both reference and advanced variants.
2. Run the algorithms through Sail in local and distributed modes with small,
   focused extension changes.
3. Qualify native and relational/DataFusion execution separately, including
   staging, memory, disk and file-descriptor behavior.
4. Validate on synthetic fixtures and real large graphs such as cit-Patents,
   Graph500 and other openly documented datasets.
5. Preserve neutral, reproducible evidence for every result, including
   failures, admission refusals, unavailable samples and timeouts.
6. Publish concise tutorials and summaries while retaining the detailed raw
   matrices and audit records.

## Project map

### Sail implementation and integration

| Directory | Role | Status and source of truth |
|---|---|---|
| `src/sail` | Upstream Sail checkout | Use for clean upstream comparisons and maintainer-facing changes. |
| `src/sail-extensions-poc` | Main extension proof of concept | Current reference for Pecan, Nutmeg integration, protocol examples, staged graph execution, and the small Sail-side API surface. Design plans and scaling notes live in `grust/docs`, not here; this repository holds code, tutorials and evidence. |
| `src/sail-large-graphs` | Large-graph execution, Argentea and traversal qualification work; branch `work/extensions-traversal-bench` | Use for real-graph ingestion, staging pressure, `ulimit`, spill behavior and distributed capacity experiments. Results must be copied into the evidence archive and summarized in the master plan. **The remote branch is the authority**: on 2026-09-28 the local checkout (`9c9ea46c8`) was 76 commits behind `querygraph/work/extensions-traversal-bench` (`b87fb27ac`) and carried five uncommitted host-file edits under `crates/sail-common-datafusion` and `crates/sail-execution`. Everything Argentea-related listed below lives on the remote tip. |
| `src/sail-querygraph-alignment` | Sail alignment and upstream compatibility checkout; branch `agent/sail-performance-alignment` | Use to compare extension assumptions with current Sail/DataFusion APIs and to stage maintainer-compatible changes. |
| `src/sail-grust-performance-alignment` | Performance-oriented Sail/Grust alignment; branch `agent/grust-performance-alignment` | Use for cross-checking execution and accounting behavior; do not treat it as the canonical published extension source unless explicitly promoted. |
| `src/sail-traversal-controls` | External traversal reference implementations: `gapbs`, `graph500`, `parallel-sssp` | A plain directory, not a git repository, holding upstream checkouts used as independent controls for BFS and SSSP. The build and pinning scripts that consume them are tracked under `sail-large-graphs/examples/extensions/benchmarks/traversal-controls`. |
| `src/sail-upstream-pr.*` | Temporary upstream PR worktrees | Ephemeral review checkouts. Never use them as a source of truth. |

The canonical extension implementation is `sail-extensions-poc`. Large-graph
work may diverge while experiments run, but every promoted change must be
reapplied to the canonical branch and recorded with its source commit.

**Where documents go.** Plans, reviews, handoffs and this map live in
`grust/docs`. The Sail fork receives only small, limited edits after careful
review; its `docs/development/extensions` tree holds the evidence reports and
design records that were written beside the code, and nothing new is added
there without that review.

Astra's gate and work checkouts live under `/private/tmp/sail-*` (about 60
detached worktrees of the same clone plus logs and receipts: 1,054 entries,
64 GB on 2026-09-28, oldest from 2026-09-16). They are Astra's to prune;
`git worktree list` in `~/src/sail` shows which are still registered.

All Sail checkouts under `~/src` are worktrees of one clone whose remotes are `origin`
(lakehq/sail, upstream), `querygraph` (the querygraph/sail fork where all
graph branches live) and `fork` (alexy/sail, a true GitHub fork, used only to
open upstream pull requests because `querygraph/sail` is not registered as a
fork).

### Execution paths

Four paths compute graph results through Sail. The names describe where the
work runs, not different definitions of the algorithms.

| Path | Entry point | Where the graph work runs | State on 2026-09-28 |
|---|---|---|---|
| **Pecan** | `pyspark_pecan.GraphAlgorithms` in `sail-extensions-poc/examples/extensions/graph-algorithms` | Python controls rounds; Sail/DataFusion executes joins and aggregates on workers; Parquet checkpoints between rounds | PageRank and WCC in reference, delta/frontier and fused-contraction forms. Ran Graph500-24 (260 M edges) in Sem's 24 GB single-host envelope and all four Graph Kernels inputs in the Morrobay campaign. No checkpoint purge, no declared layout, builtin reducers only. |
| **Nutmeg Banda** | `sail_nutmeg.Nutmeg.stage/run/drop` over `sail-extensions-poc/examples/extensions/vendor/nutmeg-graph` | Staging, CSR and Grust 0.23.0 kernels inside Sail's driver process; one machine | All 36 Grust kernels plus `pagerankDelta`, `wccRandomized`, `wccRandomizedFused`. Ran the 4 M-vertex inputs under a 32 GiB native allowance; refused them at 8 GiB; has never run Graph500-24. Limit is string identity in staging and projection, not the kernels. |
| **Nutmeg Grenada** | Nutmeg `GraphTables` adapted to `GraphAlgorithms` | Same as Pecan; no native staging | Same coverage and results as Pecan. |
| **Argentea** | `sail-large-graphs/examples/extensions/argentea` (Rust core, worker adapters, Python client) | Native CSR partitions on Sail workers for the lifetime of one Sail job; ordinary Flight shuffles carry messages; rounds unrolled as native stages in one job | Reference and residual PageRank (32-phase), BFS in reference, frontier and direction-switching forms (128-stage in process clusters), WCC (min-label and seeded star) and weighted SSSP pass process-cluster qualification; PageRank and BFS pass physical two-host (Capitola/Morrobay) qualification. Bounded rounds per job; one attempt for native regions; no capacity or performance evidence. |

The scaling review and improvement sequence for all four is
`grust/docs/FABLE-ON-ASTRA.md`; its diagnosis input is
`grust/docs/SCALING-NUTS.md`.

### Algorithm libraries and native paths

| Directory | Role |
|---|---|
| `src/nutmeg` | Nutmeg development checkout. Use for upstream Nutmeg work that has not yet been signed or frozen for Sail integration. |
| `src/nutmeg-signed` | Signed Nutmeg source used for reproducible integration and native-kernel qualification. |
| `src/grust-pagerank-fused` | Grust worktree, branch `work/pagerank-fused` (`2985fac`): one fused pull pass per PageRank iteration, bit-identical to the reference. Kernel research, not a release branch. |
| `src/grust-narrow-u32` | Grust worktree, branch `work/narrow-targets` (`48598b4`): the four-byte CSR arc targets that shipped in Grust 0.23.0. Index width is `u32` for nodes and arcs, with an explicit refusal at 2^32. |
| `src/grust-pagerank-f32` | Grust worktree, branch `work/pagerank-f32` (`ead3568`): `f32` scores behind the `precision` option. Scores default to `f64`; `f32` is opt-in and can stall against an absolute tolerance on very large N. |
| `src/grust-fastrp-f64` | Grust worktree, branch `work/fastrp-f64-accumulator` (`0e498f9`): accumulator precision for FastRP embeddings. |
| `src/grust` | This repository: shared algorithm contracts, backend-neutral documentation, release evidence and the Graph Nuts master plan. Grust 0.23.0 "Langoustine" is the released kernel set (twenty crates on crates.io, tag `v0.23.0` at `6504c0c`). `HANDOFF.md`, `GRUST-SAIL.md` and `LAKESAIL-AWS-QUERYGRAPH.md` at the repository root record the release state, the Sail changes Nutmeg needed, and where the AWS hosts' work went. |
| `src/grustframes` | Compatibility and GraphFrames-style API experiments. Use for API comparison, not as the authority for Sail staging behavior. |
| `src/graph-book` | General graph documentation and explanatory material. Link to Graph Nuts where Sail-specific execution is discussed. |

The native PageRank delta/frontier kernel is qualified only when its staging
path has also succeeded. Kernel correctness and staging scalability are
separate claims.

### Alternative extensions and comparison implementations

| Directory | Role |
|---|---|
| `src/sedona-db-extension-poc` | Apache Sedona extension proof of concept and Sail integration experiments. Keep its protocol and build instructions separate from Nutmeg. |
| `src/pecan` | Not present as a checkout. Pecan is the relational implementation and benchmark harness in `sail-extensions-poc/examples/extensions/graph-algorithms`. |
| `src/cargraph` | A plain directory, not a git repository. Graph representation and algorithm experiments that may supply fixtures or comparative kernels. |
| `src/querygraph` | QueryGraph integration and graph product work. Use for protocol and product integration, not as the benchmark evidence archive. |
| Sem's `querygraph/sail` PR 30, branch `graphframes-rs-like` (`b772a112c8`, marked do-not-merge) | Pure-PySpark Pregel with GraphX-style delta messages (`gfrs-poc/pregel.py`, `pagerank.py` with `skip_dest_state`), a checkpointer without purge, and the `sem_benchmark` harness. Runs on any Spark Connect server without an extension. Retained results: Graph500-24 in 172 s at 8.0 GB peak RSS and 7.7 GB written over 19 iterations at tolerance 1e-5 on a c5d.4xlarge with a 24 GB pool; cit-Patents in 14.7 s at 1.3 GB. Nutmeg was run only on a ten-vertex example. Not yet compared with Pecan in one envelope; `FABLE-ON-ASTRA.md` S5 schedules that. |
| `SemyonSinchenko/graphframes-rs` (external) | Rust GraphFrames-style library on DataFusion whose execution model Sem's PR follows; the Sail-side integration plan is `graphframes-rs-plan.md` and its review. |

## Benchmark and publication map

| Directory | Role |
|---|---|
| `src/adversarial-graph` | Graph benchmark harness and evidence for graph stores and execution paths. Keep workload design and comparisons neutral. |
| `src/adversarial-graph-algorithms` | Algorithm benchmark workloads, result ledgers and large-graph runs. Use for PageRank, WCC, BFS and shortest-path campaign definitions. |
| `src/adversarial-site` | Published site (`master`, `40e23a6`). `/graph/graphnuts` is the Graph Nuts page with summary findings above expandable matrices, rendered by `scripts/render-graphnuts.mjs` and checked by `scripts/verify-graphnuts-evidence.mjs`; `/graph/kernels` is the Grust kernel benchmark page from campaign B9. Both render from evidence files; hand-typed numbers fail the renderer. |
| `src/adversarial-site-large-campaign` | Branch `work/graphframes-large-campaign` (`fad121d`): the independently audited large Sail graph campaign as a site publication. Promote only verified artifacts into the main site. |
| `src/adversarial-agents` | Agent and systems benchmark support. It is not an algorithm correctness authority. |

### Hosts

| Host | Role | State on 2026-09-28 |
|---|---|---|
| morrobay | 18-core, 128 GB Intel Xeon Mac; Linux gate through Colima (`colima-sail-gate`, 24 CPUs, 64 GiB); ran the 180-trial large campaign and the 216-cell traversal campaign in 56 GiB containers | Traversal campaign `gk-traversal-release-538b` finished 2026-09-29 00:01 UTC, 216 of 216 passed (harness `538b94cbb`, host and wheel `038c9b9597`, image `f3518d652f`), evidence at `~/src/sail-extensions-gates/graph-kernels-traversal-1f18/campaign`. Its independent audit (`traversal-validation/independent-audit/audit_campaign.py` from `b87fb27ac`) was started 2026-09-29 03:30 UTC into `…/audit/`. `summarize.py` is the PageRank/WCC verifier and flags every traversal cell falsely; that output is kept under `summary-pagerank-verifier-not-applicable/`. Not a dedicated host, so its timings are observations, not publishable numbers. |
| capitola | Second physical host for Argentea two-host qualification with morrobay | Available for functional runs. |
| quegee, grust, eigen, lakecat (AWS) | quegee was the only dedicated host publishable timings came from; the others were the Linux gate and second-opinion hosts | Stopped 2026-09-23 and cleared for termination; everything unique was taken off (`grust/LAKESAIL-AWS-QUERYGRAPH.md`). No publishable timing can be produced until a dedicated host exists again. |
| c5d.4xlarge (Sem's) | Source of the cit-Patents and Graph500-24 results in PR 30 | Not ours; reproduce under our harness before citing. |

The publication pipeline is:

1. Define the workload and output contract in the benchmark repository.
2. Pin Sail, extension, native-wheel and controller commits.
3. Run local, worker and multi-host functional checks.
4. Run isolated Linux measurements with explicit memory, disk and file limits.
5. Keep every outcome and raw receipt; independently verify successful answers.
6. Promote only hash-verified summaries, figures and CSVs to
   `adversarial-site`.
7. Publish a short Graph Nuts explanation above expandable detailed matrices.

## Algorithm tracks

### PageRank

- **Reference:** full power iteration over the staged graph or relations.
- **Advanced:** delta/frontier PageRank with tolerance-scaled activation,
  reactivation, sparse message propagation and a final fixed-point certificate.
- **Native path:** Nutmeg/Grust kernel after graph staging.
- **Relational path:** Pecan/DataFusion tables with explicit intermediate
  materialization and caller-owned output.

The advanced kernel must never be credited with a staging failure. Record graph
load/staging metrics separately from iteration metrics.

### WCC

- **Reference:** minimum-label propagation for relational execution and
  union-find for native execution.
- **Advanced:** randomized contraction, with fused relational variants where
  the protocol and correctness checks remain identical.

Chain graphs are diagnostic fixtures, not representative scaling graphs:
minimum-label propagation can require a linear number of rounds there.

### BFS and shortest paths

- **Reference:** full reached-set relational relaxation, and Grust's native
  BFS and Dijkstra.
- **Advanced:** direction-optimizing push/pull BFS with compact frontier
  membership; all-edge delta-star stepping for nonnegative weighted SSSP, with
  classical delta-stepping and rho-stepping as controls.
- **Argentea:** reference, frontier and direction-switching BFS pass bounded
  worker-process and physical two-host tests; weighted SSSP passes
  process-cluster tests. The initial deployment bound was 14 levels in 32
  phases, since extended to 128 native stages in process clusters.
- **Inputs:** the Graph Kernels hub/uniform inputs reinterpreted as
  undirected with weights (`GRAPH-KERNELS-TRAVERSAL.md`), and Graph500
  Kronecker inputs streamed from the pinned upstream generator into
  partitioned Parquet (`GRAPH500.md`). Preparing an input is not a result;
  the manifest records `validation.status` until a scalable correctness check
  has run.

Exact output contracts and separate tests for disconnected, duplicate-edge and
dangling fixtures apply, as for the first two tracks.

### The other Grust kernels

Grust 0.23.0 registers 36 kernels and Nutmeg Banda exposes all of them. Only
PageRank, WCC, BFS and SSSP have relational or Argentea forms.
`grust/docs/FABLE-ON-ASTRA.md` section 6 classifies every kernel by shape (sweep,
frontier, Pregel, join, global) and says which path can carry it to what size
once the scaling sequence lands. Seven of them (closeness, harmonic,
betweenness, louvain, leiden, spanning tree, Tarjan SCC) scale only through a
variant that changes the algorithm; seven more (Yen's, all-pairs shortest
paths, DFS, articulation points, bridges, biconnected components, max-flow and
min-cut) are in-core Banda kernels and must not be described as distributed.

## Large-graph qualification

The first required real-graph campaign is:

- cit-Patents: approximately 3.7 million vertices and 16.5 million edges;
- Graph500-24 M-class: approximately 8.8 million vertices and 260 million
  edges;
- larger graphs only after staging, spill and descriptor behavior are bounded.

Nutmeg’s current failure mode is staging: sort keys, permutation buffers and a
sorted copy can exceed the configured native budget before the algorithm runs.
The qualification plan is therefore:

1. measure staging RSS, disk, temporary-file count and descriptors;
2. test deterministic no-sort/as-staged ingestion where valid;
3. add bounded partitioned staging and deterministic merges;
4. set and record `ulimit -n` for large runs;
5. rerun native PageRank/WCC and compare only like-for-like end-to-end paths.

### What has been measured at scale

The Morrobay campaign (`sail-large-graphs` remote tip,
`docs/development/extensions/pecan-nutmeg-large-benchmark.md`) is the only
place all three driver paths meet on the same inputs, envelope and timer:
180 planned trials on hub/uniform-2097152 and -4194304, 179 passes and one
timeout, all successful vectors independently audited. Its retained facts:

- At 8 GiB native / 16 GiB pool / 32 GiB container, Banda's staging of the
  33,554,395-edge uniform graph was refused: the sort requested
  23,511,075,308 bytes of workspace, about 700 bytes per edge. That record is
  an admission error, not an OOM kill. The 29 other trials of that attempt
  did not run.
- At 32 GiB native / 48 GiB pool / 56 GiB container, all paths pass. Banda's
  reference PageRank has the lowest median full-call time on all four inputs
  with higher sampled PSS. Delta/frontier is slower than reference for every
  path on every input. Relational WCC fusion cuts time 11 to 25 percent for
  6 to 24 percent more memory; native fusion does not show that.
- One Pecan reference-WCC cell timed out at 1,800 s; its two other
  repetitions and a later control passed in about 33 s. The cause is
  unexplained and the timeout stays in the evidence.

The earlier sparse campaign (`sail-extensions-poc`,
`pecan-nutmeg-benchmark.md`, 228 trials, 226 passes and two expected
convergence caps) covers the 15 path/method combinations on bounded-component
graphs up to one million vertices.

`grust/docs/FABLE-ON-ASTRA.md` reads these together with Sem's PR 30 numbers and orders
the work: separate staging from kernel measurement (S0); integer identity
through staging (S1); sort only for kernels that read order, with a spilling
sort when they do (S2); a dense integer projection without per-node strings or
a per-edge record (S3); a file-backed CSR for Banda beyond memory (S4); Pecan
purge, layout, tolerance and Sem's delta messaging as a Pecan method (S5);
Argentea continuation across jobs, then a session-scoped native-state request
to Sail (S6). Each step has a fixture and a gate.

## Checkouts and builds on Capitola

`~/src` is the operator's whole workspace; only the entries below belong to
this work, and every one of them must be listed here. On 2026-09-28 the
operator had every Cargo compile cache under `~/src` removed (467 GB across
24 `target` directories, among them `sail-extensions-poc/target/debug`,
`grust/target`, `nutmeg/target`, `grust-binding-forms/target`,
`grust/benchmarks/lsqb/target`); the disk went from 12 GB to 464 GB free.
Delivery records under `sail-extensions-poc/target/extensions-*` kept their
binaries, wheels, receipts and logs; only their `deps`, `build`,
`incremental` and `.fingerprint` caches went. Any `target/` named below as a
build is therefore gone and rebuilds on first use; a Sail host build is 40
to 50 GB.

### Sail worktrees (one clone, `~/src/sail`, remotes `origin`=lakehq, `querygraph`, `fork`=alexy)

| Directory | Branch | What is built there |
|---|---|---|
| `~/src/sail` | `lakecat` (`9f6f8065d`, 2026-08-28) | The clone itself; LakeCat work, 3 dirty files. Not a graph checkout. |
| `~/src/sail-extensions-poc` | `work/extensions-datafusion-graphs` | 12 GB after the cleanup (`target/debug`, whose arm64 binary did not start, is gone). Runnable: `target/extensions-datafusion-final/mac-x86-de8e67098/sail` (x86_64, links the uv Python `cpython-3.12.13-macos-x86_64-none`, runs under Rosetta) with its `wheels/`; `target/extensions-datafusion-development/mac-x86/sail` likewise. Wheels: `target/extensions-poc/wheels/` and `target/extensions-datafusion-development/nutmeg-wheels/` (arm64 `sail_nutmeg`). Python: `.venv` (arm64 3.12.8, PySpark 4.0.1, `sail_nutmeg` installed) and `.venvs/extensions-datafusion`, `.venvs/extensions-x86*`. Delivery records: `target/extensions-datafusion-final/README.md`, `target/extensions-distributed-poc/`, `target/pecan-benchmark/`. |
| `~/src/sail-large-graphs` | `work/s0-tiered-accounting` (from `work/extensions-traversal-bench` at `b87fb27ac`) | No `target/`; its builds run on morrobay under `~/src/sail-extensions-gates/` (for example `graph-kernels-traversal-1f18/`, the running campaign) and in Colima. The stash `preserve pre-catchup sail-large-graphs worktree 2026-09-28` holds three files that differ from the tip; audited, nothing unique but a small scheduler test difference. |
| `~/src/sail-declared-layout` | `work/declared-layout` (from `b87fb27ac`; pushed) | The graphframes-rs parity work: the `checkpointed` relation and Pecan's `layout="declared"` (pushed), and the host wrapper restating a native relation's declared layout with host columns (`crates/sail-session/src/extensions/plan.rs`, tested). Builds (session scratchpad, not `~/src`): arm64 extension artifacts in `s0-target`, the host check/test build in `host-target`, an arm64 wheel in `arm64-wheel/` and an x86_64 wheel `sail_nutmeg-0.1.0-cp312-cp312-macosx_10_12_x86_64.whl` in scratchpad `x86-wheel/` with its x86 venv `x86venv/`, made for the delivered x86 host; that host predates Pecan's utils service, so the Pecan comparison must run on morrobay. |
| (removed 2026-09-28) `sail-querygraph-alignment`, `sail-grust-performance-alignment`, `sail-upstream-pr.7kGHU1` | `agent/sail-performance-alignment`, `agent/grust-performance-alignment`, `agent/sail-performance-hot-paths` | August alignment and PR worktrees, clean and fully pushed; the worktrees were removed, the branches remain in the clone. |
| `~/src/canonical-order/sail`, `~/src/bounded-staging/sail` (symlink to the former) | detached `f1cf1729b` (the Sail commit Nutmeg 0.1.0 pins) | Astra's experiment layout from 2026-09-21, see the Nutmeg row. |

### Nutmeg and Grust

| Directory | Branch | Notes |
|---|---|---|
| `~/src/nutmeg` | `main` (`f267b03`) | Pins Sail `f1cf1729b` and Grust `6504c0c`. `target/` removed 2026-09-28. |
| `~/src/nutmeg-signed` | `work/spark-signed-integers` (`2c0813c`) | Signed-integer column handling for Spark. |
| `~/src/canonical-order/nutmeg` | `work/canonical-order` (`1492ca2`, 2026-09-21) | Astra's canonical-order staging experiment; relevant to S2. |
| `~/src/bounded-staging/nutmeg` | `work/bounded-staging` (`5220db1`, 2026-09-21) | Astra's bounded-memory staging experiment; relevant to S1/S2. Both experiment directories also link `grust` to a Claude worktree at `fd4e3ec`. |
| `~/src/grust` | `work/proposal-v5` | Evidence and book material; `target/` and `benchmarks/lsqb/target` removed 2026-09-28. |
| `~/src/grust-narrow-u32`, `grust-pagerank-f32`, `grust-fastrp-f64`, `grust-pagerank-fused` | see the Grust rows above | Kernel experiment worktrees; their `target/` caches removed 2026-09-28. |
| `~/src/grust-binding-forms`, `grust-arrow-null`, `grust-benchmark-krill`, `grust-benchmark-helix-sdk3`, `grust-release-krill`, `grust-arrow-pipeline` (+ `grust-arrow-buffer-owner`, `grust-lancedb-cancellation`), `grust-acorn-*`, `grust-copepod-delivery`, `grust-gooseneck-delivery` | various | Earlier Grust release and backend worktrees, not Graph Nuts; listed so nobody rebuilds into them by accident. `grust-binding-forms/target` (27 GB) removed 2026-09-28. |
| `~/src/grustframes` | `agent/sail-triplet-integration` | `target/` removed 2026-09-28. |
| `~/src/sedona-db-extension-poc` | `work/sail-extension-poc` | The Sedona extension, 5 dirty files. |

### Benchmarks, site, references

`~/src/adversarial-graph` (36 GB, evidence), `~/src/adversarial-graph-algorithms`
with worktrees `aga-b6` (`work/bench-b6`) and `aga-b7`
(`work/simple-rust-algo-bench-b9`), `~/src/adversarial-site` (14 dirty files),
`~/src/adversarial-site-large-campaign`, `~/src/graph-book`,
`~/src/querygraph` (32 GB), `~/src/sail-traversal-controls` (plain: `gapbs`,
`graph500`, `parallel-sssp` upstream checkouts), `~/src/cargraph` (plain),
`~/src/target` (Sail's runtime scratch: `global-logging`, `task-temp-directory`,
empty). The graphframes-rs CLI built on 2026-09-28 at `b4da56d` lives in
the session scratchpad, not under `~/src`.

## Pull requests and branches

| Where | Ref | State | What |
|---|---|---|---|
| `querygraph/sail` | `work/extensions-datafusion-graphs` (`bd8ce9ae8`) | promoted branch | The extension host, Pecan, Nutmeg wheel, sparse campaign. `sail-extensions-poc` checks it out. |
| `querygraph/sail` | `work/extensions-traversal-bench` (`b87fb27ac`) | active branch | Argentea, traversal benchmark, Graph500 preparation, the large campaign. `sail-large-graphs` checks it out, 76 behind as of 2026-09-28. |
| `querygraph/sail` | PR 30 `graphframes-rs-like` (`b772a112c8`) | open, do-not-merge | Sem's pure-PySpark Pregel and benchmark results; base is `work/extensions-datafusion-graphs`. |
| `querygraph/sail` | `work/s0-tiered-accounting` (`e33d130f8`) | pushed | S0 tiered accounting (see `grust/docs/proposals/s0-tiered-accounting/`). |
| `querygraph/sail` | `work/declared-layout` (`17f8461f1`) | pushed | The `checkpointed` relation and `checkpoint` writer (driver and distributed modes), `nutmeg_bucket` from a functions-only entry point, Pecan's declared layout, and the host wrapper restating a native relation's declared layout with host columns (first upstream PR candidate). Read side verified; write side measured slow on Sail, see `GRAPHFRAMES-RS-PARITY.md` §14. |
| `querygraph/sail` | `pr-2522`, `session-factory-hook` | rescued from AWS | Tree-verified replays of the host branches (`grust/LAKESAIL-AWS-QUERYGRAPH.md` §3–4). |
| `alexy/sail` | `csv-nanos-followup` (`e3037a5a`) | open upstream as lakehq/sail#2672 | Format-aware nanosecond widening for CSV schema inference; rebased onto merged #2522. Watch james-willis's draft #2657, which rewrites the same function. |
| `lakehq/sail` | #2630 | merged | The session factory hook: an embedder chooses the session factory. Nutmeg's only required Sail change (`nutmeg/docs/sail-prs.md`). |
| `lakehq/sail` | #2374 | merged | Delta MERGE constraints resolved by visible names. |
| `lakehq/sail` | #2400 | open | Object-store, SQL and Iceberg hot-path performance. |
| `lakehq/sail` | #2136 | closed | The earlier Sail Cypher graph query extension attempt; superseded by the extension protocol. |
| `querygraph/grust` | `work/proposal-v5` (`644e683`) | branch | The fifth revision of the Sail extension API proposal, rebuilt on the Spark Connect `Relation.extension` seam; superseded in practice by the PoC's `design-review.md` and `maintainer-request.md`. |
| `querygraph/grust` | `main` (`215cdf4` HANDOFF) | default | Grust 0.23.0 release state and handoff. |
| `querygraph/nutmeg` | `main` (`f267b03`), tag `v0.1.0` at `58a120e` | released | Nutmeg 0.1.0; pins Sail `f1cf1729b` and Grust `6504c0c`. Not on crates.io because Sail does not publish. |

## Branch and evidence discipline

Each experiment names its repository, branch and source commit. Temporary
worktrees are not evidence. A result is publishable only when the tested commit
matches the named commit, the fixture reaches the intended execution path, and
all outcomes are retained. Timing is reported with host, memory envelope,
steal, command and result boundaries. No benchmark text should describe a
system as a winner; it should state the measured conditions and limitations.

## Current document register

These are the current documents that define the design, implementation,
validation and scaling work. Paths are relative to the named repository.

### Sail extension design and implementation

- `sail-extensions-poc/docs/development/extensions/design-review.md` —
  standalone extension architecture review.
- `sail-extensions-poc/docs/development/extensions/design-review.pdf` — PDF
  rendering of that review.
- `sail-extensions-poc/docs/development/extensions/implementation-plan.md` —
  implementation plan and work breakdown.
- `sail-extensions-poc/docs/development/extensions/implementation-review.md` —
  implementation review findings.
- `sail-extensions-poc/docs/development/extensions/implementation-review-resolution.md` —
  resolutions and qualified follow-up decisions.
- `sail-extensions-poc/docs/development/extensions/abi-review.md` — wheel and
  ABI compatibility review.
- `sail-extensions-poc/docs/development/extensions/linux-environment.md` —
  Linux, Colima, Docker and host test setup.
- `sail-extensions-poc/docs/development/extensions/maintainer-request.md` —
  focused maintainer-facing change request.

### Algorithm and DataFusion plans

- `sail-extensions-poc/docs/development/extensions/datafusion-graph-plan.md` —
  direct DataFusion execution plan for graph tables.
- `sail-extensions-poc/docs/development/extensions/portable-graph-plan.md` —
  Pecan portable graph algorithm plan.
- `sail-extensions-poc/docs/development/extensions/portable-graph-validation.md` —
  validation status for portable algorithms.
- `sail-extensions-poc/docs/development/extensions/graphframes-rs-plan.md` —
  graphframes-rs integration plan.
- `sail-extensions-poc/docs/development/extensions/graphframes-rs-plan-review.md` —
  review of that plan.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark.md` —
  benchmark protocol and comparison boundaries.
- `sail-extensions-poc/examples/extensions/vendor/nutmeg-graph/OPTIMIZED_ALGORITHMS.md` —
  native advanced algorithm behavior, including delta/frontier PageRank and
  randomized WCC.
- `sail-large-graphs/docs/three-kinds-of-nut-graphs.md` — the plain-language
  explanation of Pecan, Banda and Grenada.

### Argentea (on the `sail-large-graphs` remote tip)

- `docs/development/extensions/argentea-plan.md` — the distributed Banda
  plan.
- `docs/development/extensions/argentea-integration.md` — the smallest first
  Sail change, the focused host gaps, the unrolled-round design and why the
  client loop was not kept, the live integration findings, and the remaining
  gates.
- `docs/development/extensions/argentea-advanced-plan.md` — signed residual
  PageRank, its barriers and certificate, and the BFS work.
- `docs/development/extensions/argentea-remote-fault-qualification.md` —
  worker-loss and cancellation qualification across hosts.
- `docs/development/extensions/argentea-validation/README.md` — evidence
  index for every Argentea attempt, successful and failed.
- `examples/extensions/argentea/README.md`, `PYTHON.md`, `DELTA.md`,
  `BFS.md`, `WCC_ADAPTER.md`, `SSSP_ADAPTER.md`, `GRAPH_RESOURCES.md`,
  `FAULTS.md`, `coordination.md` and the `*_CORE.md` files — tutorials and
  contracts per algorithm and per concern.

### Traversal benchmark (on the `sail-large-graphs` remote tip)

- `examples/extensions/benchmarks/TRAVERSAL-PLAN.md` — algorithm choices,
  acceptance gates, capacity ladder.
- `examples/extensions/benchmarks/GRAPH-KERNELS-TRAVERSAL.md` — BFS and SSSP
  on the four Graph Kernels inputs.
- `examples/extensions/benchmarks/GRAPH500.md` — streaming the pinned
  Graph500 generator into Parquet; explicitly not a Graph500 submission.
- `examples/extensions/benchmarks/TRAVERSAL-TUTORIAL.md` — running the
  traversal matrix.
- `docs/development/extensions/traversal-validation.md` — exact-output
  certificates for traversal campaigns.

### Scaling and benchmark evidence

- `grust/docs/SCALING-NUTS.md` — current large-graph diagnosis
  and bounded-memory staging plan.
- `grust/docs/FABLE-ON-ASTRA.md` — review of all four paths
  against the retained evidence and the ordered improvement sequence S0–S6
  with a per-kernel reach table.
- `grust/docs/GRAPHFRAMES-RS-PARITY.md` — where graphframes-rs's speed
  comes from (declared co-partitioned, sorted checkpoints), what the fork
  lacks, the ordered change to reach parity and the measurement that decides
  it; the distilled upstream candidates.
- `sail-large-graphs/docs/development/extensions/pecan-nutmeg-large-benchmark.md`
  and its `pecan-nutmeg-large-benchmark/README.md` — the 180-trial Morrobay
  campaign and its evidence index.
- `sail-large-graphs/examples/extensions/benchmarks/LARGE-GRAPHS.md` —
  reproduction guide for the large Graph Kernels inputs.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark/README.md` —
  reproducibility entry point for the published matrix.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark/primary/tables.md` —
  primary reference/advanced results.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark/fusion/tables.md` —
  matched fusion results.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark/audit/main-audit.md` —
  independent primary audit.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark/audit/fusion-audit.md` —
  independent fusion audit.
- `sail-extensions-poc/docs/development/extensions/pecan-nutmeg-benchmark/audit/constrained-audit.md` —
  constrained-resource outcomes.
- `adversarial-site/graph/graphnuts` — published Graph Nuts page, with summary
  findings above expandable detailed matrices.

### Tutorials and operational entry points

- `sail-extensions-poc/examples/extensions/TUTORIAL.md` — build and run the
  extension examples.
- `sail-extensions-poc/examples/extensions/benchmarks/TUTORIAL.md` — run local,
  worker and multi-host benchmark modes.
- `sail-extensions-poc/examples/extensions/README.md` — extension protocol and
  quick-start overview.
- `sail-extensions-poc/examples/extensions/WRITING-AN-EXTENSION.md` — minimal
  implementation examples for extension authors.
- `sail-extensions-poc/examples/extensions/sedona/README.md` and `PORTING.md` —
  Sedona-specific deployment and porting notes.
- `sail-extensions-poc/examples/extensions/nutmeg/README.md` — Nutmeg client
  and deployment notes.

### Backend-neutral Grust references

- `grust/docs/proposals/pyspark_graph_algorithms.md` — API and algorithm
  proposal for PageRank, WCC, BFS and shortest paths.
- `grust/docs/proposals/sail-extension-api.md` — extension API proposal.
- `grust/docs/proposals/sail-extension-api-astra-review.md` — review of the API
  proposal.
- `grust/docs/GENERALIZED_ALGORITHMS.md` — backend-neutral algorithm contracts.
- `grust/docs/book/chapters/algorithms-under-measurement.md` — measurement
  context for the shared algorithm surface.
- `grust/HANDOFF.md` — release and in-flight state as of 2026-09-25.
- `grust/docs/GRAPH-NUTS-HANDOFF-FABLE.md` — Astra's handoff of the Graph
  Nuts plan on 2026-09-28: finish the running Morrobay campaign untouched,
  rebuild the baseline from the traversal-bench tip, implement S0, then
  Argentea.
- `grust/GRUST-SAIL.md` and `nutmeg/docs/sail-prs.md` — the Sail changes
  Nutmeg needed and their gate.
- `grust/LAKESAIL-AWS-QUERYGRAPH.md` — what the four AWS hosts held and where
  it went.
- `grust/docs/reviews/briefing-2026-09-22.md` — the briefing that preceded
  the fifth proposal revision.
- `sail-extensions-poc/docs/development/extensions/sail-extensions-v5-opus-5.5-review.md`
  — the review that found the fifth revision proposing what the PoC had
  already built, its amendment, and the wheel question stated in full.

The register intentionally omits archived Opus drafts, superseded branch
notes, raw generated CSVs and vendored upstream Sedona documentation. Those
remain useful historical or dependency material but are not current design
authority.

### Obsolete or historical documents

Keep these until their useful conclusions have been transferred to the current
documents, then remove them in a deliberate cleanup:

- `sail-extensions-poc/docs/development/extensions/sail-extensions-v5-opus-5.5-review.md` —
  external review of an earlier design revision; historical input only.
- `sail-extensions-poc/docs/development/extensions/review-follow-up.md` —
  historical follow-up log whose resolved decisions now belong in the design
  and implementation reviews.
- `sail-extensions-poc/docs/development/extensions/graphframes-rs-plan-review.md` —
  historical review; retain while the graphframes-rs plan remains useful, then
  fold any remaining decisions into `datafusion-graph-plan.md`.
- Any document under a deleted `src/sail-extensions-*` or temporary
  `work/` checkout — branch artifact, never a source of truth.
- Generated benchmark tables, figures and raw evidence that are not referenced
  by the current manifest — retain in the evidence archive only if their
  provenance is still needed; otherwise clean them with the corresponding
  obsolete experiment.

## Immediate work queue

The ordered sequence with gates is `grust/docs/FABLE-ON-ASTRA.md` section 5; this list
maps it onto repositories.

1. **S0, in `sail-large-graphs`:** add cit-Patents and Graph500-24 to the
   `LARGE-GRAPHS.md` matrix under Sem's 24 GB envelope, with staging and
   kernel metrics as separate tiered fields. Reproduce the Banda refusal
   under our harness. Bring the local checkout up to the remote tip and
   resolve or discard its five uncommitted host edits first.
2. **S1 and S2, in `sail-extensions-poc/examples/extensions/vendor/nutmeg-graph`:**
   keep `Int64` identity through `normalize_*`; stage `asStaged` by default
   with a stable ordinal; sort only for kernels that declare they read order,
   through DataFusion's spilling sort. Gate: Graph500-24 stages in 24 GB.
3. **S3, in `grust` on a new `work/` branch beside `work/narrow-targets`:**
   an `Int64` entry to `GraphProjection` with a dense `u32` remap and no
   per-edge record unless requested. Gate: projection below 16 bytes per
   edge; Graph500-24 PageRank, WCC, BFS and Dijkstra on Banda in 24 GB.
4. **S4, in `grust` then Nutmeg:** file-backed CSR. Gate: Graph500-25 on a
   16 GB pool.
5. **S5, in `sail-extensions-poc` Pecan:** purge through a `Command.extension`
   verb now and a session temp-directory request to Sail later; a cached
   sorted edge view; mass-normalized tolerance; Sem's delta messaging as a
   third PageRank method; the four-way comparison with PR 30 on Graph500-24
   in one envelope. Ask for `AggregateUDF` in the loader.
6. **S6, in `sail-large-graphs` Argentea:** continuation across jobs through
   owner-local partition files; placement-stability measurement; the
   session-scoped native-state maintainer request in the form of
   `maintainer-request.md`.
7. **Parity thread, next:** report Sail's `partitionBy` write cost upstream
   with `partitionby_probe.py`; profile the FFI batch crossing in the
   driver checkpoint writer; rerun the shuffle-versus-declared comparison
   on morrobay with process workers once the campaign ends; then the
   four-way Graph500-24 measurement (`GRAPHFRAMES-RS-PARITY.md` §4 item 6).
8. Keep `sail-extensions-poc` as the promoted implementation branch and
   update the Graph Nuts page and its evidence manifest after each qualified
   campaign.
9. Keep this file current: every new repository, branch, document, pull
   request or host that touches graph work gets a row here in the same
   commit that creates it.
