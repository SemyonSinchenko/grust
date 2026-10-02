//! Arrow ingestion for BIGINT identity: endpoints are resolved as integers.
//!
//! The Utf8 path hashes two strings for every edge. Here a node id is an
//! `Int64`, an edge's endpoints are `Int64`, and an endpoint is resolved to its
//! node row either through a direct table, when the ids span at most
//! [`TABLE_SLOTS_PER_NODE`] slots a node, or by binary search over the sorted
//! ids. Neither hashes anything, so neither depends on a hasher's seed or on
//! what ids an input chose.
//!
//! The projection's external identity stays what the rest of the crate reads:
//! one [`NodeId`] a selected node, holding the id's decimal text. That costs a
//! string a node, not two hashes an edge.
//!
//! With no label selection every edge is kept and its position is its ordinal,
//! so the edge table is filled in place, in parallel when the execution asked
//! for workers. A label selection drops edges, so that case fills in order on
//! one thread. Both report the same error for the same input at every width:
//! the first offending edge in original order.

use std::sync::Mutex;

use arrow_schema::DataType;
use grust_core::{EdgeId, NodeId};

use super::*;

/// A direct table is used while the ids span at most this many slots a node.
const TABLE_SLOTS_PER_NODE: u128 = 16;
/// A table this small is used whatever the node count.
const SMALL_TABLE_SLOTS: u128 = 1024;
/// "No node here" in a table slot, and "not selected" in a row's dense index.
const ABSENT: u32 = u32::MAX;
/// Rows a sequential pass charges and polls at a time.
const SEQUENTIAL_STEP: usize = 1 << 14;

/// Whether these batches carry BIGINT identity: decided by the first node
/// batch's `node_id`, or the first edge batch's `source` when there is no node
/// batch. Every other batch must then agree.
pub(super) fn applies(nodes: &[RecordBatch], edges: &[RecordBatch]) -> Result<bool> {
    for (batches, name) in [(nodes, "node_id"), (edges, "source")] {
        if let Some(batch) = batches.first() {
            return Ok(column(batch, name)?.is_some_and(|c| c.data_type() == &DataType::Int64));
        }
    }
    Ok(false)
}

fn integers<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    let values: &Int64Array = column(batch, name)?
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| {
            AlgorithmError::Unsupported(format!(
                "Arrow column {name} must exist with Int64 type in every batch of a graph whose \
                 node_id is Int64"
            ))
        })?;
    non_null(values, name)?;
    Ok(values)
}

fn non_null(values: &dyn Array, name: &str) -> Result<()> {
    if values.null_count() != 0 {
        return Err(AlgorithmError::InvalidArguments(format!(
            "Arrow column {name} must not contain null"
        )));
    }
    Ok(())
}

fn labels(batch: &RecordBatch) -> Result<&StringArray> {
    let values = strings(batch, "label")?;
    non_null(values, "label")?;
    Ok(values)
}

/// Bytes of an id's decimal text, without formatting it.
fn decimal_len(id: i64) -> usize {
    let mut rest = id.unsigned_abs();
    let mut len = usize::from(id < 0) + 1;
    while rest >= 10 {
        rest /= 10;
        len += 1;
    }
    len
}

/// Node id to node row.
enum Lookup {
    /// `slots[id - base]` is the row, or [`ABSENT`].
    Table { base: i64, slots: Buffer<u32> },
    /// Ids ascending, each with its row.
    Sorted { ids: Buffer<i64>, rows: Buffer<u32> },
}

impl Lookup {
    fn build(ids: &[i64], context: &ExecutionContext) -> Result<Self> {
        let duplicate =
            |id: i64| AlgorithmError::InvalidArguments(format!("duplicate node ID: {id}"));
        let (mut least, mut greatest) = (i64::MAX, i64::MIN);
        for chunk in ids.chunks(SEQUENTIAL_STEP) {
            context.checkpoint()?;
            for &id in chunk {
                least = least.min(id);
                greatest = greatest.max(id);
            }
        }
        let span = if ids.is_empty() {
            0
        } else {
            (greatest as i128 - least as i128 + 1) as u128
        };
        if span <= (ids.len() as u128 * TABLE_SLOTS_PER_NODE).max(SMALL_TABLE_SLOTS) {
            let mut slots = Buffer::indexed(span as usize, ABSENT, context)?;
            for (first, chunk) in ids.chunks(SEQUENTIAL_STEP).enumerate() {
                context.checkpoint()?;
                for (offset, &id) in chunk.iter().enumerate() {
                    let slot = &mut slots.values[id.wrapping_sub(least) as u64 as usize];
                    if *slot != ABSENT {
                        return Err(duplicate(id));
                    }
                    *slot = (first * SEQUENTIAL_STEP + offset) as u32;
                }
            }
            return Ok(Self::Table { base: least, slots });
        }
        let mut pairs: Buffer<(i64, u32)> = Buffer::capacity(ids.len(), context)?;
        for (first, chunk) in ids.chunks(SEQUENTIAL_STEP).enumerate() {
            context.checkpoint()?;
            pairs.values.extend(
                chunk
                    .iter()
                    .enumerate()
                    .map(|(offset, &id)| (id, (first * SEQUENTIAL_STEP + offset) as u32)),
            );
        }
        // A total order on (id, row), so the sorted table is the same at every width.
        match crate::parallel::workers(context, ids.len()) {
            Some(workers) => crate::parallel::sort_total(workers, &mut pairs.values, Ord::cmp)?,
            None => pairs.values.sort_unstable(),
        }
        context.checkpoint()?;
        if let Some(pair) = pairs.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(duplicate(pair[0].0));
        }
        let mut sorted = Buffer::capacity(ids.len(), context)?;
        let mut rows = Buffer::capacity(ids.len(), context)?;
        sorted.values.extend(pairs.iter().map(|pair| pair.0));
        rows.values.extend(pairs.iter().map(|pair| pair.1));
        Ok(Self::Sorted { ids: sorted, rows })
    }

    fn row(&self, id: i64) -> Option<usize> {
        match self {
            Self::Table { base, slots } => {
                // An id below the base wraps to an offset past every slot.
                let offset = usize::try_from(id.wrapping_sub(*base) as u64).ok()?;
                slots
                    .get(offset)
                    .copied()
                    .filter(|&row| row != ABSENT)
                    .map(|row| row as usize)
            }
            Self::Sorted { ids, rows } => ids.binary_search(&id).ok().map(|at| rows[at] as usize),
        }
    }
}

/// One edge batch's structural columns, with the ordinal of its first row.
struct EdgeBatch<'a> {
    first: usize,
    sources: &'a Int64Array,
    targets: &'a Int64Array,
    labels: &'a StringArray,
    ids: &'a StringArray,
    weights: WeightColumn<'a>,
}

/// The first offending edge found so far, by original order.
#[derive(Default)]
struct FirstFailure(Mutex<Option<(usize, AlgorithmError)>>);

impl FirstFailure {
    fn record(&self, ordinal: usize, error: AlgorithmError) {
        // A poisoned lock is reported by `take`.
        if let Ok(mut held) = self.0.lock()
            && held
                .as_ref()
                .is_none_or(|(earliest, _)| ordinal < *earliest)
        {
            *held = Some((ordinal, error));
        }
    }

    fn take(self) -> Result<()> {
        match self.0.into_inner() {
            Ok(Some((_, error))) => Err(error),
            Ok(None) => Ok(()),
            Err(_) => Err(AlgorithmError::ResourceStatePoisoned),
        }
    }
}

pub(super) fn build(
    identity: SnapshotIdentity,
    node_batches: &[RecordBatch],
    edge_batches: &[RecordBatch],
    options: ProjectionOptions<'_>,
    context: &ExecutionContext,
) -> Result<GraphProjection> {
    let n = row_count(node_batches, context)?;
    let m = row_count(edge_batches, context)?;
    if n >= ABSENT as usize {
        return Err(AlgorithmError::Unsupported(format!(
            "a projection with Int64 identity holds fewer than {ABSENT} node rows; {n} were given"
        )));
    }

    // Every node row's id, in row order, and the bytes its decimal text will take.
    let mut ids: Buffer<i64> = Buffer::capacity(n, context)?;
    let mut text_bytes = 0usize;
    for batch in node_batches {
        let values = integers(batch, "node_id")?;
        labels(batch)?;
        context.charge_work(values.len())?;
        for &id in values.values() {
            text_bytes = text_bytes.saturating_add(decimal_len(id) + 2 * size_of::<usize>());
        }
        ids.values.extend_from_slice(values.values());
    }
    let lookup = Lookup::build(&ids, context)?;

    // A label selection leaves rows out, so a row's dense index is no longer the row.
    let mut selected = n;
    let dense: Option<Buffer<u32>> = match options.node_labels {
        None => None,
        Some(_) => {
            let mut dense = Buffer::capacity(n, context)?;
            selected = 0;
            for batch in node_batches {
                let labels = labels(batch)?;
                for row in 0..batch.num_rows() {
                    let keep = selects(options.node_labels, labels.value(row), context)?;
                    dense
                        .values
                        .push(if keep { selected as u32 } else { ABSENT });
                    selected += usize::from(keep);
                }
            }
            Some(dense)
        }
    };

    let edge_columns = edge_columns(edge_batches, options)?;
    for batch in &edge_columns {
        if batch.ids.null_count() != batch.ids.len() {
            context.charge_work(batch.ids.len())?;
            for id in batch.ids.iter().flatten() {
                text_bytes = text_bytes.saturating_add(id.len() + 2 * size_of::<usize>());
            }
        }
    }
    // Admit every copied id before constructing one, as the Utf8 path does.
    let id_reservation = context.reserve(text_bytes)?;
    let mut nodes: Buffer<NodeId> = Buffer::capacity(selected, context)?;
    for (first, chunk) in ids.chunks(SEQUENTIAL_STEP).enumerate() {
        context.checkpoint()?;
        for (offset, id) in chunk.iter().enumerate() {
            let row = first * SEQUENTIAL_STEP + offset;
            if dense.as_ref().is_none_or(|dense| dense[row] != ABSENT) {
                nodes.values.push(NodeId::from(id.to_string()));
            }
        }
    }
    drop(ids);

    // An endpoint: not a node at all, a node the selection left out, or a dense index.
    let resolve = |id: i64| -> Option<Option<usize>> {
        let row = lookup.row(id)?;
        Some(match &dense {
            None => Some(row),
            Some(dense) => (dense[row] != ABSENT).then_some(dense[row] as usize),
        })
    };
    let signed = options
        .weight
        .property()
        .is_some_and(|(_, _, signed)| signed);
    let (edges, weights) = if options.node_labels.is_none() && options.relationship_labels.is_none()
    {
        fill_every_edge(&edge_columns, m, options, &resolve, context)?
    } else {
        push_selected_edges(&edge_columns, m, options, &resolve, context)?
    };
    drop(lookup);
    drop(dense);
    let projection = GraphProjection::from_buffers(
        identity,
        nodes,
        edges,
        weights,
        signed,
        options.orientation,
        context,
    )?
    .with_origin(crate::ProjectionRepresentation::ArrowBatches, options)?;
    drop(id_reservation);
    Ok(projection)
}

fn edge_columns<'a>(
    batches: &'a [RecordBatch],
    options: ProjectionOptions<'_>,
) -> Result<Vec<EdgeBatch<'a>>> {
    let mut first = 0usize;
    let mut columns = Vec::new();
    columns.try_reserve_exact(batches.len())?;
    for batch in batches {
        columns.push(EdgeBatch {
            first,
            sources: integers(batch, "source")?,
            targets: integers(batch, "target")?,
            labels: labels(batch)?,
            ids: strings(batch, "edge_id")?,
            weights: WeightColumn::new(batch, options.weight)?,
        });
        first += batch.num_rows();
    }
    Ok(columns)
}

type Edges = (EdgeTable, Option<Buffer<f64>>);

/// The dense endpoints of one edge. `Ok(None)` is an edge the node selection
/// leaves out; an endpoint that is no node at all is the error.
fn endpoints(
    batch: &EdgeBatch<'_>,
    row: usize,
    resolve: &(impl Fn(i64) -> Option<Option<usize>> + Sync),
) -> Result<Option<(usize, usize)>> {
    let ordinal = batch.first + row;
    let missing =
        |end: &str| AlgorithmError::InvalidArguments(format!("edge {ordinal} has missing {end}"));
    let source = resolve(batch.sources.value(row)).ok_or_else(|| missing("source"))?;
    let target = resolve(batch.targets.value(row)).ok_or_else(|| missing("target"))?;
    Ok(source.zip(target))
}

/// The rest of a kept edge: its external id and, on a weighted projection, its weight.
fn kept(
    batch: &EdgeBatch<'_>,
    row: usize,
    options: ProjectionOptions<'_>,
) -> Result<(Option<EdgeId>, Option<f64>)> {
    let ordinal = batch.first + row;
    let weight = match options.weight.property() {
        None => None,
        Some((key, absent, signed)) => {
            let value = batch
                .weights
                .value(row)?
                .map_or_else(|| missing_weight(absent, key, ordinal), Ok)?;
            validate_weight(value, signed)?;
            Some(value)
        }
    };
    let id = (!batch.ids.is_null(row)).then(|| batch.ids.value(row).into());
    Ok((id, weight))
}

/// No label selection: every edge is kept at its ordinal, so the table is
/// filled in place, a chunk a task.
fn fill_every_edge(
    batches: &[EdgeBatch<'_>],
    m: usize,
    options: ProjectionOptions<'_>,
    resolve: &(impl Fn(i64) -> Option<Option<usize>> + Sync),
    context: &ExecutionContext,
) -> Result<Edges> {
    // The id column exists only if some edge has an id.
    let with_ids = batches
        .iter()
        .any(|batch| batch.ids.null_count() != batch.ids.len());
    let mut edges = EdgeTable::indexed(m, with_ids, context)?;
    let mut weights = match options.weight.property() {
        None => None,
        Some(_) => Some(Buffer::indexed(m, 0.0f64, context)?),
    };
    // A budget that cannot pay for every edge takes the sequential steps, so it
    // runs out at the same step with the same work counted at every width.
    let workers = if crate::parallel::work_fits(context, m)? {
        crate::parallel::workers(context, m)
    } else {
        None
    };
    let chunk = match workers {
        Some(workers) => crate::parallel::chunk_len(m, workers).min(SEQUENTIAL_STEP * 64),
        None => SEQUENTIAL_STEP,
    };
    let chunks = m.div_ceil(chunk);
    let (sources, targets, ids) = edges.columns_mut();
    let mut ids: Vec<Option<&mut [Option<EdgeId>]>> = match ids {
        Some(ids) => ids.chunks_mut(chunk).map(Some).collect(),
        None => (0..chunks).map(|_| None).collect(),
    };
    let mut weight_chunks: Vec<Option<&mut [f64]>> = match &mut weights {
        Some(weights) => weights.values.chunks_mut(chunk).map(Some).collect(),
        None => (0..chunks).map(|_| None).collect(),
    };
    let tasks: Vec<Task<'_>> = sources
        .chunks_mut(chunk)
        .zip(targets.chunks_mut(chunk))
        .zip(ids.drain(..).zip(weight_chunks.drain(..)))
        .map(|((sources, targets), (ids, weights))| Task {
            sources,
            targets,
            ids,
            weights,
        })
        .collect();
    let failure = FirstFailure::default();
    crate::parallel::for_each_owned(workers.unwrap_or(1), tasks, |index, mut task| {
        let first = index * chunk;
        let rows = task.sources.len();
        match workers {
            Some(_) => context.work_meter().charge(rows)?,
            None => context.charge_work(rows)?,
        }
        // The batch holding this chunk's first row, then onward.
        let mut at = batches.partition_point(|batch| batch.first + batch.sources.len() <= first);
        let mut filled = 0;
        while filled < rows {
            let batch = &batches[at];
            let from = first + filled - batch.first;
            let take = (batch.sources.len() - from).min(rows - filled);
            for row in from..from + take {
                let edge = endpoints(batch, row, resolve).and_then(|ends| match ends {
                    Some(ends) => Ok((ends, kept(batch, row, options)?)),
                    None => Err(AlgorithmError::OutputContract(
                        "an edge was left out although no label selection was given".into(),
                    )),
                });
                match edge {
                    Ok(((source, target), (id, weight))) => {
                        // Rows fit the stored width: `build` refused more than it holds.
                        task.sources[filled] = source as u32;
                        task.targets[filled] = target as u32;
                        if let (Some(ids), Some(id)) = (task.ids.as_deref_mut(), id) {
                            ids[filled] = Some(id);
                        }
                        if let (Some(weights), Some(weight)) = (task.weights.as_deref_mut(), weight)
                        {
                            weights[filled] = weight;
                        }
                    }
                    Err(error) => {
                        // This chunk's first failure; a later row cannot precede it.
                        failure.record(batch.first + row, error);
                        return Ok(());
                    }
                }
                filled += 1;
            }
            at += 1;
        }
        Ok(())
    })?;
    failure.take()?;
    Ok((edges, weights))
}

/// One chunk of the edge table's columns, and of the weights, for one task.
struct Task<'a> {
    sources: &'a mut [u32],
    targets: &'a mut [u32],
    ids: Option<&'a mut [Option<EdgeId>]>,
    weights: Option<&'a mut [f64]>,
}

/// A label selection: edges are dropped, so positions are not ordinals and the
/// table is pushed in order.
fn push_selected_edges(
    batches: &[EdgeBatch<'_>],
    m: usize,
    options: ProjectionOptions<'_>,
    resolve: &(impl Fn(i64) -> Option<Option<usize>> + Sync),
    context: &ExecutionContext,
) -> Result<Edges> {
    let mut edges = EdgeTable::with_capacity(m, context)?;
    let mut weights = match options.weight.property() {
        None => None,
        Some(_) => Some(Buffer::capacity(m, context)?),
    };
    for batch in batches {
        for row in 0..batch.sources.len() {
            context.charge_work(1)?;
            // Endpoints first, then the relationship label, as on the Utf8 path.
            let Some(ends) = endpoints(batch, row, resolve)? else {
                continue;
            };
            if !selects(
                options.relationship_labels,
                batch.labels.value(row),
                context,
            )? {
                continue;
            }
            let (id, weight) = kept(batch, row, options)?;
            if let (Some(weights), Some(weight)) = (&mut weights, weight) {
                weights.values.push(weight);
            }
            edges.push(ends.0, ends.1, batch.first + row, id, context)?;
        }
    }
    Ok((edges, weights))
}
