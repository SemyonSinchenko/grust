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
Last updated 2026-09-29.

## What to clone

The fork is `https://github.com/querygraph/sail.git`. Two refs matter:

| Ref | Kind | Use it for |
|---|---|---|
| `sail-extensions` | branch | the latest reviewed state; always current with the promoted extension work |
| `sail-extensions-1` | tag | the fixed first review point (commit `7c58aced5`, 2026-09-26); never moves, so comments against it stay reproducible |

For the latest:

```sh
git clone --branch sail-extensions https://github.com/querygraph/sail.git
cd sail
```

For the fixed first review:

```sh
git clone --branch sail-extensions-1 https://github.com/querygraph/sail.git
cd sail
```

A tag clone lands on a detached HEAD; that is expected. Each later review round
gets its own tag (`sail-extensions-2`, and so on) cut from the branch at the
commit handed out, so a review always has a fixed point while the branch keeps
moving.

## What to read and run

1. Read `docs/development/extensions/design-review.md`
   ([branch](https://github.com/querygraph/sail/blob/sail-extensions/docs/development/extensions/design-review.md),
   [tag](https://github.com/querygraph/sail/blob/sail-extensions-1/docs/development/extensions/design-review.md)):
   the extension architecture, the host contracts Sail needs and why, the
   package boundaries, and the evidence behind each claim.
2. Follow `examples/extensions/TUTORIAL.md`
   ([branch](https://github.com/querygraph/sail/blob/sail-extensions/examples/extensions/TUTORIAL.md),
   [tag](https://github.com/querygraph/sail/blob/sail-extensions-1/examples/extensions/TUTORIAL.md)),
   choosing the Sedona or the Nutmeg example. It covers the build on macOS or
   Linux, a local server, the two review walkthroughs, workers on one host and
   across two hosts, and the automated review checks.
3. For the graph benchmarks, `examples/extensions/benchmarks/TUTORIAL.md` runs
   Pecan, Nutmeg Banda and Nutmeg Grenada on the same inputs and measures time
   and memory in isolated Linux trials.

## What is on the branch and what is not yet

`sail-extensions` currently equals the promoted branch
`work/extensions-datafusion-graphs` at `bd8ce9ae8`: 18 commits past the first
review tag, adding the benchmark tutorial, explicit fused randomized WCC
plans, two-host worker task capacity for fused plans, and 21 changed lines of
the design review. Nothing from the 2026-09-29 capacity campaign is on it
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

Comments against the tag can cite file and line and will stay valid. Comments
against the branch should name the commit (`git rev-parse HEAD`) so they can
be placed once the branch has moved. Design questions are answered in
`FABLE-ON-ASTRA.md` section 8 and the map's work queue, and each answer that
changes code arrives as a fork branch with a reviewable copy under
`proposals/`.
