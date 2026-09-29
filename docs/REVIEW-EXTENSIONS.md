# Reviewing the Sail extensions

Sail Extensions is a way to extend Spark Connect on Sail, built after Sem
Sinchenko pointed at existing examples of extending Spark Connect. It currently
provides two extensions: Sedona (spatial SQL through Sail's native functions)
and Nutmeg, the Grust graph library. Graphs run either in colocated CSR kernels
with a unified memory pool (Nutmeg Banda) or natively in DataFusion on Sail
tables within the existing DataFusion plans (Grenada), beside the portable
relational path (Pecan) and the worker-partitioned path (Argentea).

This file is the current set of instructions for reviewers. It is kept in
step with the fork: when the review target moves, this file moves with it.
Last updated 2026-09-29 (branch-only review; Sedona entry).

## If you are here for Sedona

The Sedona extension is the smaller of the two and the better place to see
the mechanism itself: Sail resolves Sedona's spatial SQL functions through
its own native function registry, without importing a spatial engine into
the host. Read the "Sedona: reuse native functions without importing a
spatial engine" section of the design review, then run step 6 of the
tutorial ("Sedona review: spatial SQL and a shuffle"), which builds Sail
with the extension, starts a local server, runs spatial SQL from a Spark
Connect client and shows the functions surviving a shuffle. The questions
worth your time are the host contracts in the design review (a bounded
relation entry point, trusted registration with explicit compatibility,
worker identity and complete expression fields, driver placement and retry
behavior): they are what Sail would need to accept for any extension,
Sedona included, and they are where a second opinion changes the design.
Everything about graphs below can be skipped.

## What to clone

The fork is `https://github.com/querygraph/sail.git`. Review the branch
`sail-extensions`, which is always the current reviewed state:

```sh
git clone --branch sail-extensions https://github.com/querygraph/sail.git
cd sail
git rev-parse HEAD   # cite this commit in comments
```

The branch moves as work lands, so a comment names the commit it was made
against; that keeps file and line references placeable later. (The tag
`sail-extensions-1` at `7c58aced5` is the fixed point of the first review,
kept for its comments; new reviews use the branch.)

## What to read and run

1. Read [`docs/development/extensions/design-review.md`](https://github.com/querygraph/sail/blob/sail-extensions/docs/development/extensions/design-review.md):
   the extension architecture, the host contracts Sail needs and why, the
   package boundaries, and the evidence behind each claim.
2. Follow [`examples/extensions/TUTORIAL.md`](https://github.com/querygraph/sail/blob/sail-extensions/examples/extensions/TUTORIAL.md),
   choosing the Sedona or the Nutmeg example. It covers the build on macOS or
   Linux, a local server, the two review walkthroughs, workers on one host and
   across two hosts, and the automated review checks.
3. For the graph benchmarks, `examples/extensions/benchmarks/TUTORIAL.md` runs
   Pecan, Nutmeg Banda and Nutmeg Grenada on the same inputs and measures time
   and memory in isolated Linux trials.

## What is on the branch and what is not yet

`sail-extensions` currently equals the promoted branch
`work/extensions-datafusion-graphs` at `bd8ce9ae8` (the benchmark tutorial,
explicit fused randomized WCC plans, two-host worker task capacity for fused
plans, and the design review as revised for them). Nothing from the 2026-09-29 capacity campaign is on it
yet. That work (client-selectable staging order, the gRPC client decode
limit, the `max-degree` traversal source, the `argentea` harness engine,
per-iteration plan recording, the h2 keepalive knob) is on the fork's
traversal-bench line and reaches this branch, together with matching updates
to the design review and the tutorials, after our Linux benchmark gate verifies it.
The map (`GRAPH-NUTS.md`) tracks that propagation pass.

## Where the plans and findings live

Design plans, diagnoses, campaign records and this file live in `grust/docs`,
never in a Sail tree: [`GRAPH-NUTS.md`](GRAPH-NUTS.md) is the map,
[`SCALING-NUTS.md`](SCALING-NUTS.md) the large-graph diagnosis,
[`FABLE-ON-ASTRA.md`](FABLE-ON-ASTRA.md) the scaling sequence and the reply
to the design questions raised in review, and
[`reviews/gn-capacity-2026-09-29.md`](reviews/gn-capacity-2026-09-29.md) the
running capacity campaign record. Reviewable copies of individual changes,
with their patches, are under [`proposals/`](proposals/).

## Sending comments

Name the commit you reviewed (`git rev-parse HEAD`) with the file and line;
the branch moves, and the commit keeps the comment placeable. Design questions are answered in
`FABLE-ON-ASTRA.md` section 8 and the map's work queue, and each answer that
changes code arrives as a fork branch with a reviewable copy under
`proposals/`.
