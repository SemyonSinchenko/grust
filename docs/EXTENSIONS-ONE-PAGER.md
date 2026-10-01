# Sail extensions in thirty minutes: the design by module

A short form of the extension design for reviewers and for extension
authors, written 2026-09-30 after the upstream request for something
digestible: the design topics, the scope, a roadmap, the alternatives
weighed, and the minimum contract Sail would have to carry. The long form
is `docs/development/extensions/design-review.md` on the `sail-extensions`
branch of `querygraph/sail`; the author's guide beside it is
`examples/extensions/WRITING-AN-EXTENSION.md`. Clone instructions are in
[`REVIEW-EXTENSIONS.md`](REVIEW-EXTENSIONS.md).

**The sample code** is already on the branch and buildable, about 260
lines in three files, no new code was written for this page:

| File | Lines | What it shows |
|---|---|---|
| `examples/extensions/sedona/src/lib.rs` | 170 | a complete extension: Sedona's spatial functions exported to Sail as native scalar UDFs |
| `examples/extensions/nutmeg/python/sail_nutmeg/__init__.py` | 47 | discovery: the manifest and the bind call |
| `examples/extensions/WRITING-AN-EXTENSION.md`, "Minimal client" | 45 | a Spark Connect client exercising all three operation shapes |

Read Sedona's file first: it is the whole developer experience for the
most common case, functions, and it never links a Sail crate.

## The idea in one paragraph

An extension is an independent Python wheel with a native library inside.
Sail discovers it through a Python entry point, checks a manifest for
compatibility, binds it per session, and talks to it only through
DataFusion's FFI capsules: scalar UDFs, table providers, execution plans.
The extension never implements a Sail trait and never links Sail. Spark
Connect's own extension fields carry relations; functions are ordinary
calls resolved by name. Sail keeps what only a host can keep: the wire
limits, placement, replay policy, memory admission and teardown.

## Module map

| # | Module | Owner | Status on the branch |
|---|---|---|---|
| 1 | Discovery and binding | Sail session layer | implemented, flag-gated |
| 2 | Functions (expressions) | extension; Sail codec | implemented, workers included |
| 3 | Relations | Sail Connect and planner; extension handler | implemented, driver placement |
| 4 | Commands (mutations with a receipt) | extension, over module 3 | implemented |
| 5 | Placement and replay | Sail scheduler | implemented: driver, one attempt |
| 6 | Native memory | Sail pool; ABI crate | implemented |
| 7 | Lifecycle and teardown | Sail session manager | implemented; one policy open |
| 8 | Packaging and compatibility | extension; Sail loader | pinned versions, no stable ABI promise |

Each module below has the same four parts: purpose, the minimum contract,
the alternatives and the trade-off, and what is deliberately out.

### 1. Discovery and binding

*Purpose.* Find installed extensions and attach one instance to each
session without Sail knowing their domain.

*Contract.* A `pysail.extensions` entry point returns an object with
`manifest()` and `bind(session_id)` (or `bind_with_resources` when the
manifest asks for memory). The manifest declares `name`, `version`,
`api_version: 1`, `datafusion_version`, `arrow_version`, `placement`,
optional `memory_bytes`, and `relation_types` (type URL, bare payload
allowed, input arity). Duplicate names, aliases or type URLs fail
loading; catalog precedence is untouched. Enabled by
`SAIL_EXPERIMENTAL_EXTENSIONS=1`.

*Alternatives.* A Rust plugin trait linked at build time (rejected: a
trait object is not an ABI across independently compiled libraries, and
it makes every extension depend on Sail's crates and release cadence). A
pure C ABI with no Python (rejected for now: discovery, versioning and
packaging would have to be reinvented; the wheel already solves them).
Python UDF plugins (rejected: performance and no table providers).

*Out.* Hot unloading; discovery below the session layer, so the execution
layer never acquires a Python API.

### 2. Functions

*Purpose.* Let an extension's DataFusion scalar functions run on workers
as if built in.

*Contract.* `bound.scalar_udfs()` returns objects whose
`__datafusion_scalar_udf__()` yields a `datafusion_scalar_udf` capsule
holding an `FFI_ScalarUDF`. Clients call the function by name; Sail's
native-expression codec carries the function's package identity and its
complete metadata-bearing return fields to workers, which resolve the
same installed package or reject the task. Function pointers never
cross processes. Placement `any`.

*Alternatives.* The raw `Expression.extension` wire field (not
implemented: clients would need custom plan code for every call, and
name resolution already works). Driver-only scalars (rejected: a
function that cannot run on a worker is not usable in distributed Sail).

*Out.* Aggregate and window UDF registration; optimizer hooks.

*Trade-off accepted.* The codec invariant (identity plus complete fields)
reaches beyond the wrapper: a built-in expression that carries metadata,
such as WKB through a Sail builtin between two native calls, must keep
its fields too, which is why that correction lives in Sail.

### 3. Relations

*Purpose.* Let an extension produce a lazy, composable DataFrame from a
request and ordinary DataFrame inputs.

*Contract.* Spark Connect `Relation.extension` carries either a bare
payload (if the manifest allows) or Sail's envelope:
`SailExtensionRequest{payload_type_url, payload, inputs: Plan[],
envelope_version: 1}`. Sail validates version, size (8 MiB envelope,
1 MiB payload), arity (16 inputs), type URL length and nesting depth,
resolves the input plans, restores their column names, and calls
`bound.plan_relation(type_url, payload, inputs)` with named
`datafusion_execution_plan` capsules. The handler returns a
`datafusion_table_provider` capsule describing execution, not results.
Planning and EXPLAIN have no side effects. A host-owned input adapter
executes every input partition on Sail's own task context and runtime
(memory and spill policy intact) and gathers it into one host partition
for the provider.

*Alternatives.* A new RPC or gRPC service (rejected: a second protocol to
secure, version and route). SQL table functions (rejected: no DataFrame
inputs, no opaque payload). A session-factory override (exists upstream
as #2630 for embedders, but it is for replacing the session, not for
adding relations to one). Giving the extension Sail's scheduler internals
(rejected: the adapter boundary is what keeps memory policy and the
runtime in the host).

*Out.* Input expressions in the envelope (explicitly rejected on the
wire); sorted or co-partitioned foreign inputs; propagation of every host
service into foreign operators; optimizer hooks.

### 4. Commands

*Purpose.* Mutations, such as staging a graph or dropping it, without a
third protocol.

*Contract.* A command is a relation with a mutating verb whose provider
performs the mutation during execution and emits one receipt batch; the
client collects it. The result of an unacknowledged mutation is reported
as indeterminate.

*Alternatives.* The raw `Command.extension` field (not implemented: the
relation path already executes and returns rows, and the receipt makes
the outcome explicit).

*Out.* Cross-request exactly-once semantics; a general effect or
transaction system.

### 5. Placement and replay

*Purpose.* Run stateful native work where its state is, and never replay
it silently.

*Contract.* `DriverExtensionExec` is the placement boundary: a bound plan
is referenced by owner and plan identifiers, decoding checks ownership,
liveness, arity and schema, and a worker cannot decode a driver-local
handle. The scheduler gives a region that contains driver-native
execution exactly one attempt, reads included. Driver-only scalar exports
are rejected.

*Alternatives.* Running native regions on workers with ordinary retries
(deferred: it needs replicated or recoverable native state, which is the
Argentea line of work, not this contract). Automatic replay of reads
(rejected: a replay after publication and before receipt could repeat a
mutation; conservative is correct until reads and mutations are
distinguished).

*Out.* Durable state recovery; worker-placed native stages (the hooks
exist on a separate branch and are not part of this contract).

### 6. Native memory

*Purpose.* Make native allocations visible to Sail's admission instead of
competing with it invisibly.

*Contract.* The manifest's `memory_bytes` is reserved up front from the
session's DataFusion pool; the extension receives a lease through a
dependency-free ABI crate (`sail-native-resource-ffi`: version, size,
bytes, opaque owner, retain and release) and subdivides it. Native state,
snapshots and exported Arrow buffers retain the lease until their last
owner drops. A session manager owns one memory domain, shared by its
sessions and in-process workers; a separately started worker factory
owns its own.

*Alternatives.* No accounting, RSS only (rejected: the native kernels
would silently take the memory the host had promised to operators). A
separate pool (rejected: two pools cannot admit against one limit).
Exposing `Arc` or trait objects across the boundary (rejected: not an
ABI).

*Out.* Spill for native allocations (a reservation is non-spillable and
idle capacity stays reserved); a tenant security boundary; dynamic quota
lending. This is not an RSS limit: runtime, Python, transport and
metadata need their own headroom.

### 7. Lifecycle and teardown

*Purpose.* Correctness at the end: cancelled work must stop, and quota
must return exactly when its last owner is gone.

*Contract.* Executor interruption keeps a terminal identity for client
reattachment while releasing streams and buffers; session hooks drain
executors and reject late plans; the Python owner takes the GIL for final
destruction so a deferred decref cannot pin a quota; cleanup tasks drain
at shutdown even for expired sessions. Several of these fixes apply to
Sail with or without the extension flag.

*Alternatives.* Best-effort release at session end (rejected: it released
accounting while native owners could still allocate).

*Open.* The deadline and failure policy for a noncooperative native
owner: waiting preserves accounting, waiting forever is an operational
risk, and releasing early breaks ownership. This is the one unresolved
policy in the contract and a good first review topic.

### 8. Packaging and compatibility

*Purpose.* Build and ship extensions independently of Sail's tree.

*Contract.* An extension is a PyO3 wheel with its own Cargo workspace,
pinned to the host's `api_version: 1`, DataFusion 55.1.0 and Arrow
59.3.0; the loader checks those and content hashes, and rejects known
mismatches. Identical scalar wheels go on every worker; Sail validates
installed identity and does not distribute packages.

*Alternatives.* Upgrading DataFusion with the branch (rejected: the branch
pins what upstream already had and adds only `datafusion-ffi`; alignment
is separable from the domain ports).

*Out.* A stable ABI range, mixed engine-version clusters, hot swapping.
Qualified artifact pairs are recorded; nothing beyond them is promised.

## The minimum contract, as a list

What Sail core would carry, all behind the flag today:

1. `Relation.extension` dispatch with the envelope and its limits
   (module 3).
2. Scalar UDF registration from capsules, with package identity and
   complete fields in the plan codec (module 2).
3. The host-owned input adapter (module 3).
4. `DriverExtensionExec` and the one-attempt region rule (module 5).
5. The native resource bridge and the owned memory domain (module 6).
6. The lifecycle corrections (module 7), most of which stand on their own.

Everything else, the loader, manifests, Python ownership, lives in the
session layer, and all domain code lives in the extension packages.

## Roadmap

1. **Now, on `sail-extensions`:** modules 1 to 8 as described, with Sedona
   and Nutmeg as the two proofs and the reviewer's path in
   `REVIEW-EXTENSIONS.md`.
2. **Upstream in small pieces, each verifiable alone:** the lifecycle
   corrections (module 7); the codec invariant for metadata fields
   (module 2); the resource bridge ABI crate (module 6); then the
   envelope and handler contract (module 3) with its wire limits and
   parser regressions. The session-factory hook (#2630) is already merged
   and is the seam through which an embedder installs a session that
   loads extensions.
3. **Separate designs, not in this contract:** worker-placed native stages
   with recoverable state (Argentea), optimizer hooks and indexed spatial
   joins, declared co-partitioned inputs, admission policy per tenant,
   and the shutdown policy of module 7.

## Questions this page is meant to raise

1. Is the envelope (module 3) the right seam, or should relations go
   through a registered table function with the payload as an argument?
2. Should the one-attempt rule (module 5) distinguish replay-safe reads
   now, or stay conservative until worker-placed state exists?
3. Is a prepaid, non-spillable native reservation (module 6) acceptable as
   the first admission model, given that it reserves idle capacity?
4. Which shutdown policy for a stuck native owner (module 7): a bounded
   wait that reports failure, or process termination?
5. Is `api_version` plus exact DataFusion and Arrow versions (module 8)
   the compatibility statement upstream wants, or should it be a
   compatibility range from the start?
