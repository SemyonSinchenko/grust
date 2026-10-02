# Sail: ten reports for upstream, each standalone

Prepared 2026-10-02. **Nothing here has been filed.** Each folder is one
report that can be filed on its own: a description, a reproducer that needs
only a Sail binary, the output of that reproducer, the cause in the code and
a possible fix. No report depends on another, on any extension, or on
anything outside its folder.

All ten were reproduced on unmodified Sail `main` at
`99ee46f69a97342e91bf7d4eaedb4509f1d8a9c2` (2026-10-02, version 0.7.2,
DataFusion 55.1.0), release build, macOS on an Apple M1 Max, with PySpark
4.0.1 as the Spark Connect client.

| # | Report | Kind | Reproducer needs |
|---|---|---|---|
| 01 | [`checkpoint()` after a sort returns wrong results](01-checkpoint-after-sort-wrong-results/README.md) | wrong results | a checkpoint path |
| 02 | [`SORTED BY` is read as `NULLS LAST`: `ORDER BY c NULLS LAST` returns nulls first](02-catalog-sorted-by-nulls-order/README.md) | wrong result order | defaults |
| 03 | [In cluster mode, `GROUP BY` over a table with a declared sort order fails](03-cluster-group-by-over-sorted-table-fails/README.md) | query failure | `local-cluster` mode |
| 04 | [`EXPLAIN` fails for a sort-merge join that executes correctly](04-explain-fails-for-sort-merge-join/README.md) | `EXPLAIN` failure | `prefer_hash_join=false` |
| 05 | [A sort before a Delta write is removed](05-sort-before-delta-write-is-dropped/README.md) | behaviour differs between sinks | defaults |
| 06 | [`DataFrameWriter.sortBy` fails to resolve a column that exists](06-writer-sortby-cannot-resolve-column/README.md) | misleading error | defaults |
| 07 | [`repartitionByRange` hash-partitions, silently](07-repartition-by-range-is-hash/README.md) | semantic difference from Spark | defaults |
| 08 | [`spark_partition_id()` and `monotonically_increasing_id()` fail outside a projection](08-partition-id-outside-projection-fails/README.md) | query failure | defaults |
| 09 | [The sort order in a Parquet footer is never used](09-parquet-sorting-columns-never-used/README.md) | missed optimization | defaults |
| 10 | [`partitionBy` writes are 5 to 45 times slower than plain writes](10-partitionby-write-slow/README.md) | performance | defaults |

The order is by severity: answers that are wrong first, then failures, then
behaviour, then speed.

## Running a reproducer

```sh
python <folder>/repro.py /path/to/release/sail
```

Each script starts its own server on a free port with the settings its
report names, runs, prints, and stops the server. It needs
`pyspark[connect]` 4.0 and `pyarrow` in the Python that runs it. The server
embeds Python, so the script points it at that same environment.

## What was not done

- The upstream issue tracker was not searched for existing reports.
- No fix was built or tested. The "possible fix" sections come from reading
  the code.
- Nothing was run on a multi-process cluster, on Linux, or against an object
  store. Cluster evidence is `local-cluster` mode.
- Statements about Spark's behaviour come from its documentation and common
  use. Spark itself was not run.
