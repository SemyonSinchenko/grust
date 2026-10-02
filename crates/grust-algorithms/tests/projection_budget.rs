//! A projection's work and memory against the 0.23.0 release.
//!
//! 0.24.0 changed how a projection is built and what it keeps: work is charged a
//! batch at a time when the budget covers it, and the edge table is columns
//! instead of one forty-byte record an edge. Neither may change what a work
//! budget means. The work totals below were measured on the `v0.23.0` tag with
//! this same fixture and must not move; the admitted bytes were measured there
//! too and may only shrink.
#![cfg(feature = "arrow")]
use arrow_array::{ArrayRef, BooleanArray, Float64Array, RecordBatch, StringArray};
use grust_algorithms::{
    ExecutionContext, ExecutionLimits, GraphProjection, MissingWeight, Orientation, ProjectionEdge,
    ProjectionOptions, SnapshotIdentity, WeightSelection,
};
use std::sync::Arc;

fn context(workers: Option<usize>) -> ExecutionContext {
    let context = ExecutionContext::new(ExecutionLimits {
        memory_bytes: 1 << 30,
        work_units: usize::MAX,
        batch_rows: 1024,
        deadline: None,
    })
    .unwrap();
    match workers {
        Some(w) => context.with_concurrency(w).unwrap(),
        None => context,
    }
}
fn identity() -> SnapshotIdentity {
    SnapshotIdentity::new("g".into(), "r".into(), "p".into()).unwrap()
}

#[test]
fn work_is_what_0_23_charged_and_memory_is_no_more() {
    let n = 5_000usize;
    let m = 60_000usize;
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let ids: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();
    let labels: Vec<&str> = (0..n)
        .map(|i| if i % 5 == 0 { "Other" } else { "Kept" })
        .collect();
    let mut s = Vec::new();
    let mut t = Vec::new();
    let mut l = Vec::new();
    let mut e = Vec::new();
    let mut c = Vec::new();
    for edge in 0..m {
        s.push(ids[(next() % n as u64) as usize].clone());
        t.push(ids[(next() % n as u64) as usize].clone());
        l.push(if edge % 7 == 0 { "Skip" } else { "R" });
        e.push((edge % 11 == 0).then(|| format!("e{edge}")));
        c.push(Some(1.0 + (next() % 100) as f64));
    }
    let nodes = RecordBatch::try_from_iter([
        (
            "node_id",
            Arc::new(StringArray::from(ids.clone())) as ArrayRef,
        ),
        ("label", Arc::new(StringArray::from(labels)) as ArrayRef),
    ])
    .unwrap();
    let edges = RecordBatch::try_from_iter([
        ("source", Arc::new(StringArray::from(s)) as ArrayRef),
        ("target", Arc::new(StringArray::from(t)) as ArrayRef),
        ("label", Arc::new(StringArray::from(l)) as ArrayRef),
        ("edge_id", Arc::new(StringArray::from(e)) as ArrayRef),
        (
            "property.cost",
            Arc::new(Float64Array::from(c.clone())) as ArrayRef,
        ),
        (
            "present.cost",
            Arc::new(BooleanArray::from(vec![true; m])) as ArrayRef,
        ),
    ])
    .unwrap();
    let slice = |b: &RecordBatch| -> Vec<RecordBatch> {
        (0..b.num_rows())
            .step_by(1000)
            .map(|f| b.slice(f, 1000.min(b.num_rows() - f)))
            .collect()
    };
    let (nodes, edges) = (slice(&nodes), slice(&edges));
    let kept = ["Kept".to_string()];
    let rel = ["R".to_string()];
    let weight = WeightSelection::Property {
        key: "cost",
        missing: MissingWeight::Reject,
    };
    // (case, options, work units on 0.23.0, live bytes on 0.23.0, peak bytes on 0.23.0)
    let cases = [
        (
            "plain",
            ProjectionOptions::default(),
            515_066,
            3_693_567,
            3_956_455,
        ),
        (
            "undirected weighted",
            ProjectionOptions {
                orientation: Orientation::Undirected,
                weight,
                ..Default::default()
            },
            754_994,
            5_373_091,
            6_115_975,
        ),
        (
            "node labels",
            ProjectionOptions {
                node_labels: Some(&kept),
                ..Default::default()
            },
            385_120,
            3_315_766,
            3_570_626,
        ),
        (
            "both labels weighted",
            ProjectionOptions {
                node_labels: Some(&kept),
                relationship_labels: Some(&rel),
                weight,
                orientation: Orientation::Incoming,
            },
            424_118,
            3_504_139,
            4_238_970,
        ),
    ];
    for (name, options, work, live, peak) in cases {
        for workers in [None, Some(4)] {
            let context = context(workers);
            let graph =
                GraphProjection::from_arrow_batches(identity(), &nodes, &edges, options, &context)
                    .unwrap();
            let usage = context.usage().unwrap();
            assert_eq!(
                usage.work_units.counted(),
                Some(work),
                "{name}, {workers:?}"
            );
            assert!(
                usage.live_bytes <= live,
                "{name}, {workers:?}: {} live",
                usage.live_bytes
            );
            assert!(
                usage.peak_bytes <= peak,
                "{name}, {workers:?}: {} peak",
                usage.peak_bytes
            );
            drop(graph);
            assert_eq!(context.usage().unwrap().live_bytes, 0);
        }
    }
    // `from_topology`, with ordinals that are the slots and with ordinals that are not.
    for (name, shuffled, live, peak) in [
        ("topology identity ordinals", false, 4_054_569, 4_574_569),
        (
            "topology reversed ordinals with ids",
            true,
            4_155_265,
            4_675_265,
        ),
    ] {
        for workers in [None, Some(4)] {
            let context = context(workers);
            let node_ids = (0..n).map(|i| format!("n{i}").into()).collect();
            let mut y = 12345u64;
            let mut nxt = || {
                y ^= y << 13;
                y ^= y >> 7;
                y ^= y << 17;
                y
            };
            let list: Vec<ProjectionEdge> = (0..m)
                .map(|i| ProjectionEdge {
                    source: (nxt() % n as u64) as usize,
                    target: (nxt() % n as u64) as usize,
                    ordinal: if shuffled { m - 1 - i } else { i },
                    id: (shuffled && i % 13 == 0).then(|| format!("e{i}").into()),
                })
                .collect();
            let graph = GraphProjection::from_topology(
                identity(),
                node_ids,
                list,
                Some(c.iter().map(|w| w.unwrap()).collect()),
                Orientation::Outgoing,
                &context,
            )
            .unwrap();
            let usage = context.usage().unwrap();
            assert_eq!(
                usage.work_units.counted(),
                Some(445_001),
                "{name}, {workers:?}"
            );
            assert!(
                usage.live_bytes <= live,
                "{name}, {workers:?}: {} live",
                usage.live_bytes
            );
            assert!(
                usage.peak_bytes <= peak,
                "{name}, {workers:?}: {} peak",
                usage.peak_bytes
            );
            // Slots, ordinals and ids read back as given.
            let last = graph.edges().get(m - 1);
            assert_eq!(last.ordinal, if shuffled { 0 } else { m - 1 });
            assert_eq!(graph.edges().get(13).id.is_some(), shuffled);
            assert_eq!(graph.edges().iter().len(), m);
            drop(graph);
        }
    }
}
