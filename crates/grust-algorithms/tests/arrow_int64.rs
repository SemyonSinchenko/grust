//! BIGINT identity on the Arrow path, with the Utf8 path as its oracle.
//!
//! The same graph is handed over twice, ids as `Int64` and ids as their decimal
//! text, and the two projections must be the same projection: node order and
//! identity, every edge's endpoints, ordinal and id, the weights a kernel reads,
//! the recorded selection, and the first error for a bad input. The graphs are
//! large enough to cross the parallel floor, and every case runs with no
//! concurrency asked for, with one worker and with four.
#![cfg(feature = "arrow")]

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use grust_algorithms::{
    AlgorithmError, ExecutionContext, ExecutionLimits, GraphProjection, MissingWeight, Orientation,
    ProjectionOptions, SnapshotIdentity, WeightSelection, degree, dijkstra,
};

const WIDTHS: [Option<usize>; 3] = [None, Some(1), Some(4)];

fn context(workers: Option<usize>, work_units: usize) -> ExecutionContext {
    let context = ExecutionContext::new(ExecutionLimits {
        memory_bytes: 1 << 28,
        work_units,
        batch_rows: 1024,
        deadline: None,
    })
    .unwrap();
    match workers {
        Some(workers) => context.with_concurrency(workers).unwrap(),
        None => context,
    }
}

fn identity() -> SnapshotIdentity {
    SnapshotIdentity::new("g".into(), "r1".into(), "reader".into()).unwrap()
}

/// A deterministic stream; the tests need variety, not randomness.
struct Stream(u64);

impl Stream {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

/// A graph as plain columns, so it can be handed over with either identity.
struct Columns {
    node_ids: Vec<i64>,
    node_labels: Vec<&'static str>,
    sources: Vec<i64>,
    targets: Vec<i64>,
    edge_labels: Vec<&'static str>,
    edge_ids: Vec<Option<String>>,
    costs: Vec<Option<f64>>,
}

/// `nodes` nodes and `edges` edges with parallel edges, loops and isolates.
/// `stride` spreads the ids: 1 keeps them compact (the direct table), a large
/// stride with a negative start makes them sparse (the sorted lookup).
fn columns(nodes: usize, edges: usize, stride: i64, seed: u64) -> Columns {
    let mut stream = Stream(seed);
    let node_ids: Vec<i64> = (0..nodes as i64)
        .map(|i| (i - nodes as i64 / 2) * stride)
        .collect();
    // Shuffle node rows so a row is not its id's rank.
    let mut order: Vec<usize> = (0..nodes).collect();
    for i in (1..nodes).rev() {
        order.swap(i, stream.below(i + 1));
    }
    let node_ids: Vec<i64> = order.iter().map(|&i| node_ids[i]).collect();
    let node_labels = (0..nodes)
        .map(|i| if i % 5 == 0 { "Other" } else { "Kept" })
        .collect();
    // The last tenth of the nodes are isolates.
    let connected = nodes - nodes / 10;
    let mut columns = Columns {
        node_ids,
        node_labels,
        sources: Vec::new(),
        targets: Vec::new(),
        edge_labels: Vec::new(),
        edge_ids: Vec::new(),
        costs: Vec::new(),
    };
    for edge in 0..edges {
        let source = columns.node_ids[stream.below(connected)];
        let target = if edge % 97 == 0 {
            source
        } else {
            columns.node_ids[stream.below(connected)]
        };
        columns.sources.push(source);
        columns.targets.push(target);
        columns
            .edge_labels
            .push(if edge % 7 == 0 { "Skip" } else { "R" });
        columns
            .edge_ids
            .push((edge % 11 == 0).then(|| format!("e{edge}")));
        columns
            .costs
            .push(Some(1.0 + (stream.next() % 1000) as f64 / 8.0));
    }
    columns
}

fn ids(values: &[i64], text: bool) -> ArrayRef {
    if text {
        Arc::new(StringArray::from_iter_values(
            values.iter().map(i64::to_string),
        ))
    } else {
        Arc::new(Int64Array::from(values.to_vec()))
    }
}

/// The columns as node and edge batches of at most `rows` rows each.
fn batches(columns: &Columns, text: bool, rows: usize) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
    let nodes = RecordBatch::try_from_iter([
        ("node_id", ids(&columns.node_ids, text)),
        (
            "label",
            Arc::new(StringArray::from(columns.node_labels.clone())) as ArrayRef,
        ),
    ])
    .unwrap();
    let edges = RecordBatch::try_from_iter([
        ("source", ids(&columns.sources, text)),
        ("target", ids(&columns.targets, text)),
        (
            "label",
            Arc::new(StringArray::from(columns.edge_labels.clone())) as ArrayRef,
        ),
        (
            "edge_id",
            Arc::new(StringArray::from(columns.edge_ids.clone())) as ArrayRef,
        ),
        (
            "property.cost",
            Arc::new(Float64Array::from(columns.costs.clone())) as ArrayRef,
        ),
        (
            "present.cost",
            Arc::new(BooleanArray::from(
                columns
                    .costs
                    .iter()
                    .map(Option::is_some)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let slice = |batch: &RecordBatch| -> Vec<RecordBatch> {
        (0..batch.num_rows())
            .step_by(rows)
            .map(|first| batch.slice(first, rows.min(batch.num_rows() - first)))
            .collect()
    };
    (slice(&nodes), slice(&edges))
}

fn build(
    columns: &Columns,
    text: bool,
    options: ProjectionOptions<'_>,
    context: &ExecutionContext,
) -> Result<GraphProjection, AlgorithmError> {
    let (nodes, edges) = batches(columns, text, 1000);
    GraphProjection::from_arrow_batches(identity(), &nodes, &edges, options, context)
}

fn assert_same(integer: &GraphProjection, text: &GraphProjection) {
    assert_eq!(integer.node_ids(), text.node_ids());
    assert_eq!(integer.edge_count(), text.edge_count());
    assert!(
        integer.edges().iter().eq(text.edges()),
        "same endpoints, ordinals and ids"
    );
    assert_eq!(integer.representation(), text.representation());
    assert_eq!(integer.orientation(), text.orientation());
    assert_eq!(integer.is_weighted(), text.is_weighted());
    let (a, b) = (integer.selection().unwrap(), text.selection().unwrap());
    assert_eq!(
        (
            &a.node_labels,
            &a.relationship_labels,
            &a.weight_property,
            a.missing_weight
        ),
        (
            &b.node_labels,
            &b.relationship_labels,
            &b.weight_property,
            b.missing_weight
        )
    );
    assert_eq!(
        degree(integer).unwrap().counts(),
        degree(text).unwrap().counts()
    );
    if let Some(source) = integer.node_ids().first() {
        // Dijkstra reads every weight on the paths it settles.
        assert_eq!(
            dijkstra(integer, source.as_str()).unwrap().values(),
            dijkstra(text, source.as_str()).unwrap().values()
        );
    }
}

fn weighted() -> WeightSelection<'static> {
    WeightSelection::Property {
        key: "cost",
        missing: MissingWeight::Reject,
    }
}

#[test]
fn integer_identity_builds_the_projection_the_text_identity_builds() {
    let kept = ["Kept".to_string()];
    let relationships = ["R".to_string()];
    let selections = [
        ProjectionOptions::default(),
        ProjectionOptions {
            orientation: Orientation::Undirected,
            weight: weighted(),
            ..Default::default()
        },
        ProjectionOptions {
            node_labels: Some(&kept),
            weight: weighted(),
            ..Default::default()
        },
        ProjectionOptions {
            relationship_labels: Some(&relationships),
            ..Default::default()
        },
        ProjectionOptions {
            node_labels: Some(&kept),
            relationship_labels: Some(&relationships),
            orientation: Orientation::Incoming,
            weight: weighted(),
        },
    ];
    // Compact ids take the direct table; a stride of a billion from a negative
    // start takes the sorted lookup.
    for stride in [1, 1_000_000_007] {
        let columns = columns(3_000, 40_000, stride, 0x9E37_79B9_7F4A_7C15);
        for options in selections {
            let oracle = context(None, usize::MAX);
            let text = build(&columns, true, options, &oracle).unwrap();
            for workers in WIDTHS {
                let context = context(workers, usize::MAX);
                let integer = build(&columns, false, options, &context).unwrap();
                assert_same(&integer, &text);
                drop(integer);
                assert_eq!(context.usage().unwrap().live_bytes, 0, "nothing retained");
            }
        }
    }
}

#[test]
fn a_node_id_is_its_decimal_text_and_is_a_valid_kernel_source() {
    let columns = Columns {
        node_ids: vec![i64::MIN, -7, 0, 42, i64::MAX],
        node_labels: vec!["N"; 5],
        sources: vec![i64::MIN, -7, 0, 42],
        targets: vec![-7, 0, 42, i64::MAX],
        edge_labels: vec!["R"; 4],
        edge_ids: vec![None; 4],
        costs: vec![Some(1.0); 4],
    };
    let context = context(None, usize::MAX);
    let graph = build(&columns, false, ProjectionOptions::default(), &context).unwrap();
    let names: Vec<&str> = graph.node_ids().iter().map(|id| id.as_str()).collect();
    assert_eq!(
        names,
        [
            "-9223372036854775808",
            "-7",
            "0",
            "42",
            "9223372036854775807"
        ]
    );
    let reached = dijkstra(&graph, "-9223372036854775808").unwrap();
    assert_eq!(reached.values(), [0.0, 1.0, 2.0, 3.0, 4.0]);
}

/// Replace one value of an id column, to make exactly one row bad.
fn with(mut values: Vec<i64>, at: usize, value: i64) -> Vec<i64> {
    values[at] = value;
    values
}

#[test]
fn the_first_bad_edge_in_original_order_is_the_error_at_every_width() {
    let good = columns(2_000, 40_000, 1, 7);
    let stranger = 1_000_000;
    for workers in WIDTHS {
        // Two missing endpoints far apart: the earlier one is reported, and a
        // missing source is named before a missing target on the same edge.
        let mut bad = columns(2_000, 40_000, 1, 7);
        bad.sources = with(bad.sources, 31_000, stranger);
        bad.targets = with(bad.targets, 9_000, stranger);
        bad.sources = with(bad.sources, 9_000, stranger);
        let error = build(
            &bad,
            false,
            ProjectionOptions::default(),
            &context(workers, usize::MAX),
        );
        assert!(
            matches!(&error, Err(AlgorithmError::InvalidArguments(message)) if message == "edge 9000 has missing source"),
            "{:?}",
            error.err()
        );
        let text = build(
            &bad,
            true,
            ProjectionOptions::default(),
            &context(None, usize::MAX),
        );
        assert_eq!(
            format!("{:?}", error.err()),
            format!("{:?}", text.err()),
            "the Utf8 path agrees"
        );

        // An absent weight under Reject, after a later missing endpoint.
        let mut bad = columns(2_000, 40_000, 1, 7);
        bad.costs[12_345] = None;
        bad.targets = with(bad.targets, 30_000, stranger);
        let options = ProjectionOptions {
            weight: weighted(),
            ..Default::default()
        };
        let error = build(&bad, false, options, &context(workers, usize::MAX));
        assert!(
            matches!(&error, Err(AlgorithmError::InvalidArguments(message)) if message.contains("edge 12345 ")),
            "{:?}",
            error.err()
        );

        // A duplicate node id, through the table and through the sorted lookup.
        for stride in [1, 1_000_000_007] {
            let mut bad = columns(2_000, 100, stride, 7);
            bad.node_ids[1_500] = bad.node_ids[3];
            let duplicate = bad.node_ids[3];
            let context = context(workers, usize::MAX);
            let error = build(&bad, false, ProjectionOptions::default(), &context);
            assert!(
                matches!(&error, Err(AlgorithmError::InvalidArguments(message)) if *message == format!("duplicate node ID: {duplicate}")),
                "{:?}",
                error.err()
            );
            assert_eq!(
                context.usage().unwrap().live_bytes,
                0,
                "a refused build retains nothing"
            );
        }
    }
    assert!(
        build(
            &good,
            false,
            ProjectionOptions::default(),
            &context(None, usize::MAX)
        )
        .is_ok()
    );
}

#[test]
fn nulls_and_mixed_identity_types_are_refused() {
    let columns = columns(50, 200, 1, 3);
    let context = context(None, usize::MAX);
    let (nodes, edges) = batches(&columns, false, 1000);
    let (text_nodes, text_edges) = batches(&columns, true, 1000);
    // Int64 nodes with Utf8 endpoints, and the reverse.
    for (nodes, edges) in [(&nodes, &text_edges), (&text_nodes, &edges)] {
        assert!(matches!(
            GraphProjection::from_arrow_batches(
                identity(),
                nodes,
                edges,
                ProjectionOptions::default(),
                &context
            ),
            Err(AlgorithmError::Unsupported(_))
        ));
    }
    let null_at = |values: &[i64], at: usize| -> ArrayRef {
        Arc::new(Int64Array::from(
            values
                .iter()
                .enumerate()
                .map(|(i, v)| (i != at).then_some(*v))
                .collect::<Vec<_>>(),
        ))
    };
    // Rebuilt by name, so the replaced column's field becomes nullable.
    let replace = |batch: &RecordBatch, name: &str, column: ArrayRef| -> RecordBatch {
        let schema = batch.schema();
        RecordBatch::try_from_iter(schema.fields().iter().zip(batch.columns()).map(
            |(field, array)| {
                let array = if field.name() == name {
                    column.clone()
                } else {
                    array.clone()
                };
                (field.name().as_str(), array)
            },
        ))
        .unwrap()
    };
    let bad_nodes = [replace(&nodes[0], "node_id", null_at(&columns.node_ids, 4))];
    let bad_edges = [replace(&edges[0], "target", null_at(&columns.targets, 9))];
    for (nodes, edges, name) in [
        (&bad_nodes[..], &edges[..], "node_id"),
        (&nodes[..], &bad_edges[..], "target"),
    ] {
        let error = GraphProjection::from_arrow_batches(
            identity(),
            nodes,
            edges,
            ProjectionOptions::default(),
            &context,
        );
        assert!(
            matches!(&error, Err(AlgorithmError::InvalidArguments(message)) if *message == format!("Arrow column {name} must not contain null")),
            "{:?}",
            error.err()
        );
    }
    assert_eq!(context.usage().unwrap().live_bytes, 0);
}

#[test]
fn an_empty_graph_and_a_graph_without_edges_build() {
    let context = context(Some(4), usize::MAX);
    let none = columns(0, 0, 1, 1);
    let graph = build(&none, false, ProjectionOptions::default(), &context).unwrap();
    assert_eq!((graph.node_count(), graph.edge_count()), (0, 0));
    let isolated = columns(100, 0, 1, 1);
    let graph = build(&isolated, false, ProjectionOptions::default(), &context).unwrap();
    assert_eq!((graph.node_count(), graph.edge_count()), (100, 0));
    assert_eq!(graph.node_ids().len(), 100);
}

#[test]
fn a_work_budget_too_small_is_refused_the_same_way_at_every_width() {
    let columns = columns(3_000, 40_000, 1, 11);
    let full = context(None, usize::MAX);
    let needed = {
        let graph = build(&columns, false, ProjectionOptions::default(), &full).unwrap();
        drop(graph);
        full.usage().unwrap().work_units.counted().unwrap()
    };
    let mut left_behind = Vec::new();
    for workers in WIDTHS {
        let context = context(workers, needed / 2);
        let error = build(&columns, false, ProjectionOptions::default(), &context);
        assert!(matches!(
            error,
            Err(AlgorithmError::BudgetExceeded {
                resource: "work",
                ..
            })
        ));
        assert_eq!(context.usage().unwrap().live_bytes, 0);
        left_behind.push(context.usage().unwrap().work_units.counted().unwrap());
        // And a budget that just pays builds, with the same work counted.
        let exact = self::context(workers, needed);
        build(&columns, false, ProjectionOptions::default(), &exact).unwrap();
        assert_eq!(exact.usage().unwrap().work_units.counted().unwrap(), needed);
    }
    assert!(
        left_behind.windows(2).all(|pair| pair[0] == pair[1]),
        "{left_behind:?}"
    );
}
