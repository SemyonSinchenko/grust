# Projection ingest

What `GraphProjection::from_arrow_batches` costs over a real graph's vertex and
edge files: the wall time of the build and the bytes the projection's execution
admits. Reading the files is timed separately and is not part of the build.

The helper is outside the Grust workspace, like the other benchmarks. It
depends on `grust-algorithms` by path and on the `parquet` crate.

```sh
cargo build --release
target/release/projection-ingest \
  --vertices graph500-24-v.parquet --edges graph500-24-e.parquet \
  --src source --dst target \
  --ids int64 --workers 8 --orientation outgoing --kernel wcc
```

- The inputs hold a BIGINT `id` and two BIGINT endpoint columns, as the LDBC
  Graphalytics Parquet files do.
- `--ids int64` hands the ids over as they are. `--ids utf8` casts them to
  decimal text first, which is what an embedder without integer identity does
  and the only form 0.23.0 accepts.
- `--workers 0` leaves the execution's concurrency unset, which runs the
  sequential code. `--workers 1` runs the parallel implementation on one
  thread.
- The batches follow the grust-arrow layout, 8,192 rows each, with an empty
  label and a null `edge_id`.
- One JSON object is printed: the phases, the admitted bytes, and a checksum
  (node, edge and arc counts, the largest out-degree, and with `--kernel wcc`
  the number of weak components). The checksum must not change with the id
  type or the worker count, and in the records here it does not.

## Recorded runs

`evidence/m1-max-2026-10-02.jsonl` is this branch; `evidence/m1-max-2026-10-02-v0.23.0.jsonl`
is the same helper built against the `v0.23.0` tag. Both on one Apple M1 Max
laptop (8 performance and 2 efficiency cores, macOS), a warm page cache, other
applications running. **These are a laptop's numbers: they show the shape of a
change, not a result to quote.** Median of three runs unless marked; the three
agree within 5% except where a range is given.

Inputs are the LDBC Graphalytics files `cit-Patents` (3,774,768 nodes,
16,518,947 edges) and `graph500-24` (8,870,942 nodes, 260,379,520 edges).

Build seconds, outgoing orientation:

| Graph | Version, ids | No concurrency | 1 worker | 4 workers | 8 workers | Admitted |
|---|---|---:|---:|---:|---:|---:|
| cit-Patents | 0.23.0, Utf8 | 8.09 | | | 8.03 | 1.13 GiB |
| cit-Patents | 0.24.0, Utf8 | 6.54 | 6.27 | 6.24 | 6.05 | 0.57 GiB |
| cit-Patents | 0.24.0, Int64 | 0.86 | 0.74 | 0.42 | 0.34 | 0.57 GiB |
| graph500-24 | 0.23.0, Utf8 | 119.0 (one run) | | | | 13.38 GiB |
| graph500-24 | 0.24.0, Utf8 | 106.3 (one run) | | | | 4.65 GiB |
| graph500-24 | 0.24.0, Int64 | 14.84 | 13.09 | 4.19 | 3.10 | 4.65 GiB |

Build seconds, undirected orientation (twice the arcs):

| Graph | Version, ids | No concurrency | 1 worker | 4 workers | 8 workers | Admitted |
|---|---|---:|---:|---:|---:|---:|
| cit-Patents | 0.23.0, Utf8 | 8.95 | | | 7.85 | 1.31 GiB |
| cit-Patents | 0.24.0, Utf8 | 7.41 | 6.56 | 6.58 | 6.55 | 0.70 GiB |
| cit-Patents | 0.24.0, Int64 | 2.01 | 1.02 | 0.63 | 0.53 | 0.70 GiB |
| graph500-24 | 0.23.0, Utf8 | 144.1 (one run) | | | | 16.29 GiB |
| graph500-24 | 0.24.0, Utf8 | 129.7 (one run) | | | | 6.59 GiB |
| graph500-24 | 0.24.0, Int64 | 37.43 | 19.92 (19.73 to 21.00) | 6.65 (6.61 to 6.99) | 4.70 (4.70 to 5.74) | 6.59 GiB |

What the records do not cover: a dedicated host, a Linux host, sparse integer
ids (both graphs take the direct table), a label selection, weights, and edge
ids. `crates/grust-algorithms/tests/arrow_int64.rs` covers those for
correctness, not for time.
