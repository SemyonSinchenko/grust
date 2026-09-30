# The relational stream loss: status for review with Astra

Written 2026-09-30 for a joint review. The goal of the review is one thing:
find what closes the stream. This file states what is observed, what has
been ruled out and how, what is only hypothesis, and the three cheapest
experiments that separate the hypotheses. Every number comes from a cell
receipt or server log on Morrobay; the campaign record is
[`reviews/gn-capacity-2026-09-29.md`](reviews/gn-capacity-2026-09-29.md)
and the decision guide that this failure limits is
[`WHICH-PATH.md`](WHICH-PATH.md).

## 1. The symptom

A relational traversal (Pecan or Grenada) on Graph500 scale 24 or 25, in
Sail's process-cluster mode (one driver, two worker processes, 32
partitions, in one 32-core, 100 GiB Linux container), fails in the first
heavy iteration with

```
pyspark.errors.exceptions.connect.SparkRuntimeException:
  h2 protocol error: error reading a body from connection
```

The driver log is silent until the failure. Its only errors are teardown
noise after the session is removed: the driver's own calls to stop tasks
on a worker fail with `tonic::transport::Error(Transport, hyper::Error(Io,
Kind(ConnectionReset)))`, and workers retrying a report log `invalid
argument: driver 1 not found`. No line from any process names the stream,
the peer, or the reason before the client sees the error.

## 2. Where it happens

Fifteen relational cells, none with a worker replaced mid-run. "Into the
iteration" is the failure time minus the failing iteration's start, from
the receipt's iteration events and the server log (approximate to a few
seconds).

| Build | Cell | Fails in | Into the iteration | Peak PSS | `memory.max` events |
|---|---|---|---|---|---|
| baseline | s24 Grenada BFS reference | iteration 2 | 108 s | 42.8 GiB | 0 |
| baseline | s24 Pecan BFS frontier | iteration 2 | 146 s | 51.6 GiB | 0 |
| baseline | s24 Grenada BFS frontier | iteration 2 | 445 s | 91.9 GiB | 0 |
| baseline | s24 Grenada SSSP reference | iteration 2 | 258 s | 39.1 GiB | 0 |
| baseline | s24 Grenada SSSP frontier | iteration 2 | 307 s | 42.0 GiB | 0 |
| baseline | s24 Grenada SSSP delta-star | bucket 2 | 494 s | 36.8 GiB | 0 |
| baseline | s24 Pecan SSSP frontier | iteration 2 | 302 s | 37.2 GiB | 0 |
| baseline | s24 Pecan SSSP reference | iteration 2 | 486 s | 20.4 GiB | 0 |
| baseline | s25 Grenada SSSP reference | iteration 2 | 1142 s | 65.3 GiB | 0 |
| baseline | s25 Pecan SSSP reference | iteration 3 | 255 s | 65.8 GiB | 0 |
| baseline | s25 Grenada BFS reference | iteration 3 | 1135 s | 86.8 GiB | 1610 |
| baseline | s25 Pecan BFS reference | iteration 4 | 279 s | 93.1 GiB | 3767 |
| baseline | s24 Pecan BFS reference | iteration 2 | 466 s | 99.9 GiB | 1130, and `oom_kill` 1 |
| gate 3 | s24 Pecan SSSP delta-star | iteration 2 | 1082 s | 65.0 GiB | 0 |
| gate 3 | s25 Pecan BFS reference | iteration 2 | 616 s | 40.7 GiB | 0 |

Three of the baseline rows sit at the container limit and one has a
kernel OOM kill, so memory explains or confounds those. The other eleven
fail with 34 to 80 GiB of headroom and no memory event at all. Those
eleven are the problem.

What passes on the same inputs, same builds, same container:

| Cell | Result |
|---|---|
| s24 Pecan BFS push-pull | passed, 844 s |
| s24 Grenada BFS push-pull | passed, 775 s |
| s25 Pecan BFS frontier (baseline, and again on gate 2) | passed, 2729 s at 75 GiB; its iteration 2 alone ran 949 s |
| s25 Pecan and Grenada BFS push-pull | traversal complete in 1516 to 1610 s |
| every cit-Patents cell (16.5M edges) | 29 of 30 passed |
| every Banda and single-host Argentea cell at scale 24 | passed (different execution paths, same Sail cluster) |

So the failing shape is an iteration whose expansion join sees a frontier
of millions of vertices against the 537M-row adjacency (iteration 2 from
the hub: 407,203 reached after iteration 1 at scale 24; at scale 25,
640,062 after iteration 1 and 14.6M after iteration 2). Push-pull never runs that join in the push direction, and
passes. A 949 s iteration passed at scale 25 while a 108 s one failed at
scale 24, so neither scale nor duration alone decides it.

## 3. Established

Each item names its evidence.

1. **Both workers are alive when the stream dies.** The harness samples
   every Sail process each 50 ms. In the four cells checked (gate 3 s25
   Pecan BFS reference; gate 3 s24 Pecan SSSP delta-star; baseline s24
   Grenada BFS reference; baseline s24 Pecan SSSP reference) both worker
   pids are present, holding their memory (up to 31 and 33 GiB), until 0
   to 2 s after the execute phase ends. No worker crashed, aborted or was
   killed first. The `ConnectionReset` errors are the driver talking to
   workers that are already shutting down.
2. **It is not memory in eleven cells.** No `memory.max` reclaim event, no
   OOM kill, 20 to 66 GiB peak in a 100 GiB container.
3. **It is not a fixed timer.** The failing iteration ran between 108 s
   and 1142 s before the loss.
4. **It is not the worker idle probe.** Sail removes a worker whose task
   slots have been vacant for `cluster.worker_max_idle_time_secs` (default
   60), and that did kill one Argentea cell with this same error text
   (worker 1 stopped at 3.8 minutes, replaced ten minutes later). But no
   relational cell shows a worker stopped or started mid-run, and the two
   gate-3 relational failures ran with the probe disabled (86400 s).
5. **It is not the 4 MiB client decode limit.** That limit is real (Sail's
   internal gRPC clients kept Tonic's default while its servers accept
   128 MiB), it produced a different, explicit message (`decoded message
   length too large`), it is fixed on `work/grpc-client-decode-limit` and
   verified, and gate 3 contains the fix.
6. **It is not the hash join's build side.** Recorded plans showed the
   partitioned hash join building on the adjacency; `work/s5-frontier-build-side`
   puts the frontier on the left. Gate 3 contains it, and the reference
   variant still loses its stream (and earlier than on the baseline at
   scale 25: iteration 2 instead of iteration 4).
7. **Lengthening the keepalive timeout alone did not cure it.** Sail's
   gRPC servers ping every 60 s and drop a peer that misses a 10 s window
   (`sail-common/src/server/builder.rs`). Gate 3 ran with the window at
   120 s (interval still 60 s) through a fork-only knob, and both gate-3
   relational cells still failed. The gate-3 scale-24 SSSP delta-star cell
   ran 1082 s into its failing iteration against 494 s for the baseline's
   delta-star cell (Grenada), which is suggestive and no more than that.
8. **Stream creation timeout and task attempts are not involved.** The
   failures occur with the stream timeout at 60 s and at 900 s alike.

One earlier statement in the campaign record is wrong and is corrected
here: worker processes do log into the driver's `server.log` at `info`
(the `sail_execution::worker::actor` lines are theirs). What is missing is
any worker-side warning or error, because a task failure is reported to
the driver only as a status, and the driver logs statuses at `debug`.

## 4. Same error text, different causes already explained

These are closed; they are listed so they are not re-diagnosed.

| Cell | Cause |
|---|---|
| gate 3, Argentea BFS frontier, scale 24 | the idle probe removed worker 1 with its shuffle output; the extension job runs with one attempt |
| baseline, Pecan BFS reference, scale 24 | kernel OOM kill of a worker at the 100 GiB limit |
| gate 3, Argentea BFS reference, scale 25, first attempt | the host was in a swap storm under a second VM; the retry without it ran to an OOM kill at 2 h 14 min instead |

## 5. Hypotheses

Ranked by how well they fit section 3. None is verified.

**H1. HTTP/2 keepalive, on a peer too busy to answer.** Every Sail gRPC
server sends pings; a peer whose tokio runtime is saturated by the join
(32 tokio and 32 rayon threads on 32 cores) misses the window and the
server closes the connection, and every stream on it ends with exactly
this error. Fits: only heavy iterations fail; workers alive; variable
time. Against: the 120 s window did not cure it. Not yet tested: keepalive
effectively off (interval and timeout both very large) on every server.
The one Argentea cell run with 300 s and 600 s did not lose a stream, but
it is a different job and died of memory.

**H2. HTTP/2 reset-stream protection.** The shuffle between 32 map and 32
reduce partitions opens about 1024 streams per worker pair. The `h2`
crate (0.4.15 in the lock file; limits read from the 0.4.19 source)
closes a connection with
`ENHANCE_YOUR_CALM` when a peer resets too many streams: more than 20
pending-accept resets from the remote side, or 1024 locally reset streams.
A consumer that drops many task streams at once (a join side finishing,
a cancelled stage, a limit) issues that many resets. One two-host Argentea
run logged `locally-reset streams reached limit (1024)` on the driver at
the moment it failed. Fits: depends on partition count and plan shape, not
on time or memory; push-pull has a different plan and passes. Not tested.

**H3. A flow-control or window stall that a timeout then converts to a
reset.** Servers run with `http2_adaptive_window`. Least specific, listed
for completeness.

**H4. A second size limit on a path the decode fix did not cover.** The
fix raised the decode limit on the clients built in
`sail-execution/src/rpc.rs`. If the worker-to-worker stream client
(`sail-execution/src/stream/service/client.rs`, `fetch_task_stream`, a
Flight `do_get`) or an encode limit takes another path, a large batch in
the heavy iteration could end the stream. Against: the limit error has
its own message and none was seen. Cheap to check by reading the code.

## 6. Experiments

Each is one cell of about ten minutes on the existing gate 3 build.
Reproducer: scale-24 Pecan BFS frontier (failed 146 s into iteration 2,
at 467 s wall clock on the baseline) or scale-24 Grenada BFS reference
(481 s). Neither has been rerun on gate 3 yet, so the first run also
confirms the reproducer there.

1. **See the close.** Rerun the reproducer with connection-level logging
   on every process: `RUST_LOG=info,h2::proto::connection=debug,h2::proto::streams=warn,hyper::proto::h2=debug,sail_execution::task_runner=debug,sail_execution::stream=debug`.
   The harness pins `RUST_LOG='info'` in `examples/extensions/benchmarks/runtime.py`
   (the `server` function), so this needs a one-line knob on a fork
   branch. Expected: a `GOAWAY` with its reason and the side that sent
   it, and the first task that failed with its cause. This alone may end
   the search.
2. **Keepalive off.** Same cell with
   `SAIL_EXPERIMENTAL_HTTP2_KEEPALIVE_INTERVAL_SECS=86400` and the timeout
   at 86400 through the matrix `environment` map and
   `--http2-keepalive-timeout`. Passes: H1. Fails the same way: H1 is out.
3. **Fewer streams.** Same cell with 16 partitions (256 streams per pair).
   Passes where 32 fails: H2, or something else that scales with stream
   count.

The matrix harness takes a one-cell configuration; the decision cells are
templates (`gn-decide3-gate3.json` on Morrobay is Pecan BFS reference at
scale 25; change the suite's dataset and variant). Container settings go
in its `environment` map (`SAIL_*` names only), harness arguments in
`extra_cell_args`.

## 7. Where the evidence is

On Morrobay, under `~/src/sail-extensions-gates/`:

| What | Path |
|---|---|
| Baseline cells (13 failures, 3 passes) | `graph-nuts-b87fb27ac/capacity-hub/cells/<cell>/artifacts/{receipt.json,server.log,memory-samples.jsonl}` |
| Gate 3 relational failures | `graph-nuts-gate-next/decide-gate3/cells/decide-pecan-s24-sssp-…/` and `graph-nuts-gate-next/decide3-gate3/cells/decide-pecan-s25-bfs-…/` (these receipts also carry the per-iteration physical plans) |
| Idle-probe case | `graph-nuts-gate-next/argentea-first-gate3/cells/capacity-bfs-argentea-r1-scale24-argentea-bfs-frontier/` |
| Two-host run with the reset-limit warning | Capitola scratchpad, `gate-next/two-host/scale24-bfs-reference-lan2/server-and-workers.log` |
| Gate 3 build | `/targets/graph-nuts-ffcfbd569/` in the `sail-extension-targets` volume (`colima-sail-gate` context); source `work/s5-frontier-build-side` at `ffcfbd569` on `querygraph/sail` |
| Harness on the host | `graph-nuts-gate-next/harness-gate3/examples/extensions/benchmarks/run_matrix.py` (with the `environment` map, `work/matrix-environment`) |

Sail source, in the fork checkout:

| Concern | Location |
|---|---|
| Server keepalive, adaptive window, the two fork-only knobs | `crates/sail-common/src/server/builder.rs` |
| Internal gRPC clients and their message limits | `crates/sail-execution/src/rpc.rs` |
| Worker-to-worker task stream fetch | `crates/sail-execution/src/stream/service/client.rs` |
| Idle and lost-worker probes | `crates/sail-execution/src/driver/worker_pool/core.rs`, `driver/actor/handler.rs` |
| Task status reporting (where a failure's cause is dropped to a status) | `crates/sail-execution/src/task_runner/actor/handler.rs` |

## 8. Questions for the review

1. Does Astra's build or audit work already have connection-level logs
   from a failing cell, or a known `h2` reset limit hit in Sail?
2. Is there a reason the keepalive cannot simply be disabled inside a
   local cluster, where a dead peer is detected by the worker heartbeat
   anyway?
3. Should a failed task's cause be logged at `warn` on the worker and
   the driver? It would have made this a one-run diagnosis, and it is a
   small, upstreamable change.
4. Which of the three experiments first, given a shared host: my order
   is 1, then 2 or 3 depending on what 1 shows.
