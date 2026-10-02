//! Time `GraphProjection::from_arrow_batches` over Parquet vertex and edge files.
//!
//!     projection-ingest --vertices V.parquet --edges E.parquet [--src src] [--dst dst]
//!         [--ids utf8|int64] [--workers N] [--orientation outgoing|undirected]
//!         [--memory-gib G] [--kernel wcc]
//!
//! The files hold BIGINT `id` and two BIGINT endpoint columns, as the LDBC
//! Graphalytics Parquet files do. The batches handed to the projection follow
//! the grust-arrow layout: `node_id` and `label` for nodes; `source`, `target`,
//! `label` and a null `edge_id` for edges. With `--ids utf8` the ids are cast to
//! strings first, which is what an embedder without integer identity does; with
//! `--ids int64` they stay BIGINT. Reading the files is timed separately and is
//! not part of the build. `--workers 0` leaves the execution's concurrency
//! unset, which runs the sequential code.
//!
//! Prints one JSON object: the phases, the projection's admitted bytes, and a
//! checksum (node count, edge count, arc count, largest degree) that must not
//! change with the id type or the worker count.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use grust_algorithms::{
    ExecutionContext, ExecutionLimits, GraphProjection, Orientation, ProjectionOptions,
    SnapshotIdentity, degree, weakly_connected_components,
};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rayon::prelude::*;

fn parquet_files(path: &Path) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path.to_path_buf()];
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    files.sort();
    files
}

/// The named BIGINT columns of every row group, in file and row-group order.
fn read(path: &Path, names: &[&str]) -> Vec<Vec<Int64Array>> {
    let mut tasks = Vec::new();
    for file in parquet_files(path) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&file).unwrap()).unwrap();
        for group in 0..builder.metadata().num_row_groups() {
            tasks.push((file.clone(), group));
        }
    }
    tasks
        .par_iter()
        .flat_map_iter(|(file, group)| {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(file).unwrap()).unwrap();
            let schema = builder.parquet_schema();
            let leaves: Vec<usize> = names
                .iter()
                .map(|name| {
                    schema
                        .columns()
                        .iter()
                        .position(|c| c.name() == *name)
                        .unwrap_or_else(|| panic!("{}: no column {name}", file.display()))
                })
                .collect();
            let mask = ProjectionMask::leaves(schema, leaves);
            let reader = builder
                .with_row_groups(vec![*group])
                .with_projection(mask)
                .with_batch_size(8192)
                .build()
                .unwrap();
            reader
                .map(|batch| {
                    let batch = batch.unwrap();
                    names
                        .iter()
                        .map(|name| {
                            batch
                                .column_by_name(name)
                                .unwrap()
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap_or_else(|| panic!("column {name} is not BIGINT"))
                                .clone()
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn id_column(values: &Int64Array, utf8: bool) -> (Field, ArrayRef) {
    if utf8 {
        let strings: StringArray = values.iter().map(|v| v.map(|v| v.to_string())).collect();
        (Field::new("", DataType::Utf8, true), Arc::new(strings))
    } else {
        (Field::new("", DataType::Int64, true), Arc::new(values.clone()))
    }
}

fn labels(rows: usize) -> ArrayRef {
    Arc::new(StringArray::from(vec![""; rows]))
}

fn batch(columns: Vec<(&str, Field, ArrayRef)>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, field, _)| field.clone().with_name(*name))
        .collect();
    let arrays = columns.into_iter().map(|(_, _, array)| array).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn option<'a>(args: &'a [String], name: &str, default: &'a str) -> &'a str {
    args.iter()
        .position(|a| a == name)
        .map(|i| args[i + 1].as_str())
        .unwrap_or(default)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let vertices = PathBuf::from(option(&args, "--vertices", ""));
    let edges = PathBuf::from(option(&args, "--edges", ""));
    let (src, dst) = (option(&args, "--src", "src"), option(&args, "--dst", "dst"));
    let utf8 = match option(&args, "--ids", "utf8") {
        "utf8" => true,
        "int64" => false,
        other => panic!("--ids {other}: utf8 or int64"),
    };
    let workers: usize = option(&args, "--workers", "0").parse().unwrap();
    let orientation = match option(&args, "--orientation", "outgoing") {
        "outgoing" => Orientation::Outgoing,
        "undirected" => Orientation::Undirected,
        other => panic!("--orientation {other}: outgoing or undirected"),
    };
    let memory_gib: usize = option(&args, "--memory-gib", "48").parse().unwrap();
    let kernel = option(&args, "--kernel", "");

    let started = Instant::now();
    let node_columns = read(&vertices, &["id"]);
    let edge_columns = read(&edges, &[src, dst]);
    let read_seconds = started.elapsed().as_secs_f64();

    // Building the batches is the embedder's work (a cast to strings, or none).
    let shaping = Instant::now();
    let text = Field::new("", DataType::Utf8, true);
    let node_batches: Vec<RecordBatch> = node_columns
        .par_iter()
        .map(|columns| {
            let (field, ids) = id_column(&columns[0], utf8);
            let rows = ids.len();
            batch(vec![("node_id", field, ids), ("label", text.clone(), labels(rows))])
        })
        .collect();
    let edge_batches: Vec<RecordBatch> = edge_columns
        .par_iter()
        .map(|columns| {
            let (source_field, sources) = id_column(&columns[0], utf8);
            let (target_field, targets) = id_column(&columns[1], utf8);
            let rows = sources.len();
            let absent: ArrayRef = Arc::new(StringArray::new_null(rows));
            batch(vec![
                ("source", source_field, sources),
                ("target", target_field, targets),
                ("label", text.clone(), labels(rows)),
                ("edge_id", text.clone(), absent),
            ])
        })
        .collect();
    drop((node_columns, edge_columns));
    let shape_seconds = shaping.elapsed().as_secs_f64();

    let limits = ExecutionLimits {
        memory_bytes: memory_gib << 30,
        work_units: usize::MAX,
        batch_rows: 8192,
        deadline: None,
    };
    let mut context = ExecutionContext::new(limits).unwrap();
    if workers > 0 {
        context = context.with_concurrency(workers).unwrap();
    }
    let building = Instant::now();
    let graph = GraphProjection::from_arrow_batches(
        SnapshotIdentity::new("g".into(), "r".into(), "bench".into()).unwrap(),
        &node_batches,
        &edge_batches,
        ProjectionOptions { orientation, ..ProjectionOptions::default() },
        &context,
    )
    .unwrap_or_else(|e| panic!("projection: {e}"));
    let build_seconds = building.elapsed().as_secs_f64();
    let usage = context.usage().unwrap();

    let checking = Instant::now();
    let degrees = degree(&graph).unwrap();
    let largest = degrees.counts().iter().copied().max().unwrap_or(0);
    let arcs: usize = degrees.counts().iter().sum();
    let components = (kernel == "wcc").then(|| {
        let found = weakly_connected_components(&graph).unwrap();
        let mut ids: Vec<usize> = found.values().to_vec();
        ids.sort_unstable();
        ids.dedup();
        ids.len()
    });
    let check_seconds = checking.elapsed().as_secs_f64();
    println!(
        "{{\"ids\": \"{}\", \"workers\": {workers}, \"orientation\": \"{orientation:?}\", \
         \"nodes\": {}, \"edges\": {}, \"arcs\": {arcs}, \"largest_out_degree\": {largest}, \
         \"components\": {}, \"read_seconds\": {read_seconds:.3}, \"shape_seconds\": {shape_seconds:.3}, \
         \"build_seconds\": {build_seconds:.3}, \"check_seconds\": {check_seconds:.3}, \
         \"live_bytes\": {}, \"peak_bytes\": {}, \"first_node_id\": \"{}\"}}",
        if utf8 { "utf8" } else { "int64" },
        graph.node_count(),
        graph.edge_count(),
        components.map_or("null".to_string(), |c| c.to_string()),
        usage.live_bytes,
        usage.peak_bytes,
        graph.node_ids().first().map_or("", |id| id.as_str()),
    );
}
