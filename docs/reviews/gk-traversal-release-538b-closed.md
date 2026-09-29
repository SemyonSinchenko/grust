# Campaign gk-traversal-release-538b: closed 2026-09-29T03:36:31.205806+00:00

The BFS/SSSP campaign on the four Graph Kernels inputs finished on
2026-09-29 00:01:44 UTC with every planned cell passed, and its independent
audit passed every cell. Nothing in the evidence was changed after the run.

| Item | Value |
| --- | --- |
| Run ID | gk-traversal-release-538b |
| Planned / completed / outcomes | 216 / 216 / {'passed': 216} |
| Launch | 2026-09-28T05:37:58.596911+00:00, pid 28382, `/usr/bin/python3 /Users/alexy/src/sail-extensions-gates/graph-kernels-traversal-1f18/host-harness/run_matrix.py --config /Users/alexy/src/sail-extensions-gates/graph-kernels-traversal-1f18/preparation/matrix-final.json --skip-prepare` |
| Configuration sha256 (launch pin) | ce41b082ca95eb70280463e15bbdd02df4560b4e23b594b14c779469447e8b17 |
| Harness source | 538b94cbb99298f94667cde29e94b3e4cefe60fc (`work/extensions-traversal-bench` lineage) |
| Runtime (host) source | 038c9b9597d3fcf7e0b8c30c1253d7d77563f012 |
| Native wheel source | 038c9b9597d3fcf7e0b8c30c1253d7d77563f012 |
| Image | sha256:f3518d652fbea8b9b9f9ebf9849277ca3bd39d0c21bb2c6e36fbaeaa2dc2678e (`sail-pecan-benchmark:release`) |
| Host | morrobay, Colima profile `colima-sail-gate` (24 CPUs, 64 GiB); Docker context `colima-sail-gate` |
| Container limits per cell | cpus 16, cpuset 0-15, memory 56 GiB, no swap, outer timeout 2400 s |
| Descriptor limit (`ulimit -n`) in the image | 1024 (container default; the harness at this revision did not record it per cell) |
| Defaults | partitions 16, threads 16, task slots 64, Sail pool 51539607552 B, native quota 34359738368 B, delta 4.0 |
| Evidence | `/Users/alexy/src/sail-extensions-gates/graph-kernels-traversal-1f18/campaign` (cells/, configuration.json, matrix-results.json sha256 `5d473adc0cde1ae2069c68f78ee430582bd36dfb4ac96cd53ddebde2e360b23b`) |
| Independent audit | `/Users/alexy/src/sail-extensions-gates/graph-kernels-traversal-1f18/audit/audit-report.json` sha256 `9ae42253cede55e04b3125f8962d26515f7a1796c90960b6624f9c2ff8c20deb`, {'passed': 216}; tool `docs/development/extensions/traversal-validation/independent-audit/audit_campaign.py` at `b87fb27ac` (`/Users/alexy/src/sail-extensions-gates/graph-kernels-traversal-1f18/tip-audit/`), run read-only in the campaign image with the datasets from the target volume, configuration pin ce41b082ca95eb70280463e15bbdd02df4560b4e23b594b14c779469447e8b17 |
| Not applicable | `summarize.py` (the PageRank/WCC verifier) flags every traversal cell for structural reasons; its output is kept under `summary-pagerank-verifier-not-applicable/` and is not a result |

Boundaries: complete-call timings from input handles to written results, on a
shared host under Colima; observations, not publishable numbers. Argentea is
not part of these cells. The report for these results belongs with the
fork's `docs/development/extensions` evidence tree, written from this
directory and the audit report.
