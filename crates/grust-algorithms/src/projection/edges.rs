//! The edge table a projection keeps beside its adjacency, as columns.
//!
//! A kernel that names an edge names it by *slot*, its position here, and
//! reads its endpoints, its original ordinal and its external id through
//! [`Edges`]. The table used to be one [`ProjectionEdge`] an edge: three
//! `usize` and an optional string, forty bytes, of which most projections use
//! eight. It is now four columns, two of them optional:
//!
//! - **endpoints**, four bytes each, the width the adjacency stores a target
//!   in, so the same node-count bound covers both;
//! - **ordinals**, only when some edge's ordinal is not its slot. An input
//!   handed over whole, with nothing left out by a label selection, has none;
//! - **external ids**, only when some edge has one.
//!
//! So a graph of plain `(source, target)` rows keeps eight bytes an edge.

use grust_core::EdgeId;

use super::*;

pub(crate) struct EdgeTable {
    sources: Buffer<Target>,
    targets: Buffer<Target>,
    /// Absent when every edge's ordinal is its slot.
    ordinals: Option<Buffer<usize>>,
    /// Absent when no edge has an external id.
    ids: Option<Buffer<Option<EdgeId>>>,
}

/// The error for an endpoint no node table this crate builds could hold.
fn outside() -> ProcedureError {
    ProcedureError::InvalidArguments("edge endpoint outside node table".into())
}

impl EdgeTable {
    /// An empty table with room for `capacity` edges, to [`Self::push`] into.
    pub(crate) fn with_capacity(capacity: usize, context: &ExecutionContext) -> Result<Self> {
        Ok(Self {
            sources: Buffer::capacity(capacity, context)?,
            targets: Buffer::capacity(capacity, context)?,
            ordinals: None,
            ids: None,
        })
    }

    /// A table of `count` edges whose ordinals are their slots, endpoints to be
    /// written through [`Self::columns_mut`]; `with_ids` adds the id column.
    pub(crate) fn indexed(
        count: usize,
        with_ids: bool,
        context: &ExecutionContext,
    ) -> Result<Self> {
        Ok(Self {
            sources: Buffer::indexed(count, 0, context)?,
            targets: Buffer::indexed(count, 0, context)?,
            ordinals: None,
            ids: match with_ids {
                true => Some(Buffer::indexed(count, None, context)?),
                false => None,
            },
        })
    }

    /// The endpoint columns and, when present, the id column, for a caller
    /// that fills disjoint ranges of them.
    #[allow(clippy::type_complexity)]
    pub(crate) fn columns_mut(
        &mut self,
    ) -> (&mut [Target], &mut [Target], Option<&mut [Option<EdgeId>]>) {
        (
            &mut self.sources.values,
            &mut self.targets.values,
            self.ids.as_mut().map(|ids| &mut ids.values[..]),
        )
    }

    /// Append an edge. The optional columns appear with the first edge that
    /// needs them, admitted for the table's whole capacity then.
    pub(crate) fn push(
        &mut self,
        source: usize,
        target: usize,
        ordinal: usize,
        id: Option<EdgeId>,
        context: &ExecutionContext,
    ) -> Result<()> {
        let slot = self.sources.values.len();
        let capacity = self.sources.values.capacity().max(slot + 1);
        if ordinal != slot && self.ordinals.is_none() {
            let mut ordinals = Buffer::capacity(capacity, context)?;
            ordinals.values.extend(0..slot);
            self.ordinals = Some(ordinals);
        }
        if id.is_some() && self.ids.is_none() {
            let mut ids = Buffer::capacity(capacity, context)?;
            ids.values.resize(slot, None);
            self.ids = Some(ids);
        }
        self.sources
            .values
            .push(Target::try_from(source).map_err(|_| outside())?);
        self.targets
            .values
            .push(Target::try_from(target).map_err(|_| outside())?);
        if let Some(ordinals) = &mut self.ordinals {
            ordinals.values.push(ordinal);
        }
        if let Some(ids) = &mut self.ids {
            ids.values.push(id);
        }
        Ok(())
    }

    /// The table for caller-supplied edges, which it consumes.
    pub(crate) fn from_edges(
        edges: Vec<ProjectionEdge>,
        context: &ExecutionContext,
    ) -> Result<Self> {
        let mut table = Self::with_capacity(edges.len(), context)?;
        for (slot, edge) in edges.into_iter().enumerate() {
            if slot % 1024 == 0 {
                context.checkpoint()?;
            }
            table.push(edge.source, edge.target, edge.ordinal, edge.id, context)?;
        }
        Ok(table)
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.sources.values.len()
    }

    /// Every edge's source, as stored. Widen with `as usize`.
    #[inline(always)]
    pub(crate) fn sources(&self) -> &[Target] {
        &self.sources.values
    }

    /// Every edge's target, as stored. Widen with `as usize`.
    #[inline(always)]
    pub(crate) fn targets(&self) -> &[Target] {
        &self.targets.values
    }

    /// The ordinals, when some edge's is not its slot.
    pub(crate) fn ordinals(&self) -> Option<&[usize]> {
        self.ordinals.as_deref()
    }

    /// The external ids, when some edge has one.
    pub(crate) fn ids(&self) -> Option<&[Option<EdgeId>]> {
        self.ids.as_deref()
    }
}

/// One edge of a projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgeRef<'a> {
    /// Source row in the projection's node table.
    pub source: usize,
    /// Target row in the projection's node table.
    pub target: usize,
    /// Original snapshot edge position, unique within the projection.
    pub ordinal: usize,
    /// External edge identity, when the edge has one.
    pub id: Option<&'a EdgeId>,
}

/// A projection's edges, by slot. Kernel edge slots index this.
#[derive(Clone, Copy)]
pub struct Edges<'a> {
    table: &'a EdgeTable,
}

impl<'a> Edges<'a> {
    pub(crate) fn new(table: &'a EdgeTable) -> Self {
        Self { table }
    }

    /// Number of edges.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Whether the projection has no edge.
    pub fn is_empty(&self) -> bool {
        self.table.len() == 0
    }

    /// The edge at `slot`.
    ///
    /// # Panics
    /// When `slot` is not below [`Self::len`], as a slice index would.
    #[inline]
    pub fn get(&self, slot: usize) -> EdgeRef<'a> {
        EdgeRef {
            source: self.table.sources.values[slot] as usize,
            target: self.table.targets.values[slot] as usize,
            ordinal: self.ordinal(slot),
            id: self
                .table
                .ids
                .as_ref()
                .and_then(|ids| ids.values[slot].as_ref()),
        }
    }

    /// The original ordinal of the edge at `slot`.
    ///
    /// # Panics
    /// When `slot` is not below [`Self::len`].
    #[inline]
    pub fn ordinal(&self, slot: usize) -> usize {
        match &self.table.ordinals {
            Some(ordinals) => ordinals.values[slot],
            None => {
                assert!(slot < self.table.len(), "edge slot out of range");
                slot
            }
        }
    }

    /// Every edge, in slot order.
    pub fn iter(&self) -> EdgeIter<'a> {
        EdgeIter {
            edges: *self,
            slots: 0..self.len(),
        }
    }

    /// Every edge's source, as stored, for a pass that reads the column.
    pub(crate) fn sources(&self) -> &'a [Target] {
        self.table.sources()
    }

    /// Every edge's target, as stored, for a pass that reads the column.
    pub(crate) fn targets(&self) -> &'a [Target] {
        self.table.targets()
    }
}

/// Every edge of a projection, in slot order.
#[derive(Clone)]
pub struct EdgeIter<'a> {
    edges: Edges<'a>,
    slots: std::ops::Range<usize>,
}

impl<'a> Iterator for EdgeIter<'a> {
    type Item = EdgeRef<'a>;

    fn next(&mut self) -> Option<EdgeRef<'a>> {
        self.slots.next().map(|slot| self.edges.get(slot))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.slots.size_hint()
    }
}

impl ExactSizeIterator for EdgeIter<'_> {}

impl<'a> IntoIterator for Edges<'a> {
    type Item = EdgeRef<'a>;
    type IntoIter = EdgeIter<'a>;

    fn into_iter(self) -> EdgeIter<'a> {
        self.iter()
    }
}
