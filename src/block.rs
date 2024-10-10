//! `HeaderBlock`, `PrimitiveBlock` and `PrimitiveGroup`s

use crate::dense::DenseNodeIter;
use crate::elements::{Element, Node, Relation, Way};
use crate::error::{new_error, ErrorKind, Result};
use crate::proto::osmformat;
use std;

/// A `HeaderBlock`. It contains metadata about following [`PrimitiveBlock`]s.
#[derive(Clone, Debug)]
pub struct HeaderBlock {
    header: osmformat::HeaderBlock,
}

impl HeaderBlock {
    pub(crate) fn new(header: osmformat::HeaderBlock) -> HeaderBlock {
        HeaderBlock { header }
    }

    /// Returns the (optional) bounding box of the included features.
    pub fn bbox(&self) -> Option<HeaderBBox> {
        self.header.bbox.as_ref().map(|bbox| HeaderBBox {
            left: (bbox.left() as f64) * 1.0_e-9,
            right: (bbox.right() as f64) * 1.0_e-9,
            top: (bbox.top() as f64) * 1.0_e-9,
            bottom: (bbox.bottom() as f64) * 1.0_e-9,
        })
    }

    /// Returns a list of required features that a parser needs to implement to parse the following
    /// [`PrimitiveBlock`]s.
    pub fn required_features(&self) -> &[String] {
        self.header.required_features.as_slice()
    }

    /// Returns a list of optional features that a parser can choose to ignore.
    pub fn optional_features(&self) -> &[String] {
        self.header.optional_features.as_slice()
    }

    /// Returns the name of the program that generated the file or `None` if unset.
    pub fn writing_program(&self) -> Option<&str> {
        if self.header.has_writingprogram() {
            Some(self.header.writingprogram())
        } else {
            None
        }
    }

    /// Returns the source of the `bbox` field or `None` if unset.
    pub fn source(&self) -> Option<&str> {
        if self.header.has_source() {
            Some(self.header.source())
        } else {
            None
        }
    }

    /// Returns the replication timestamp of the file, or `None` if unset.
    /// The timestamp is expressed in seconds since the UNIX epoch.
    pub fn osmosis_replication_timestamp(&self) -> Option<i64> {
        if self.header.has_osmosis_replication_timestamp() {
            Some(self.header.osmosis_replication_timestamp())
        } else {
            None
        }
    }

    /// Returns the replication sequence number of the file, or `None` if unset.
    pub fn osmosis_replication_sequence_number(&self) -> Option<i64> {
        if self.header.has_osmosis_replication_sequence_number() {
            Some(self.header.osmosis_replication_sequence_number())
        } else {
            None
        }
    }

    /// Returns the replication base URL of the file, or `None` if unset.
    pub fn osmosis_replication_base_url(&self) -> Option<&str> {
        if self.header.has_osmosis_replication_base_url() {
            Some(self.header.osmosis_replication_base_url())
        } else {
            None
        }
    }
}

/// A bounding box that is usually included in a [`HeaderBlock`].
/// The maximum precision of the coordinates is one nanodegree (10⁻⁹).
#[derive(Clone, Debug)]
pub struct HeaderBBox {
    /// left coordinate in degrees (minimum longitude)
    pub left: f64,
    /// right coordinate in degrees (maximum longitude)
    pub right: f64,
    /// top coordinate in degrees (minimum latitude)
    pub top: f64,
    /// bottom coordinate in degrees (maximum latitude)
    pub bottom: f64,
}

/// A `PrimitiveBlock`. It contains a sequence of groups.
#[derive(Clone, Debug)]
pub struct PrimitiveBlock {
    block: osmformat::PrimitiveBlock,
}

impl PrimitiveBlock {
    pub(crate) fn new(block: osmformat::PrimitiveBlock) -> PrimitiveBlock {
        PrimitiveBlock { block }
    }

    /// Returns an iterator over the groups in this `PrimitiveBlock`.
    pub fn groups(&self) -> impl Iterator<Item = PrimitiveGroup> {
        self.block
            .primitivegroup
            .iter()
            .map(|g| PrimitiveGroup::new(&self.block, g))
    }

    /// Returns an iterator over the elements in this `PrimitiveBlock`.
    pub fn elements(&self) -> impl Iterator<Item = Element> {
        self.groups().map(|g| g.elements()).flatten()
    }

    /// Calls the given closure on each element.
    pub fn for_each_element<F>(&self, f: F)
    where
        F: for<'a> FnMut(Element<'a>),
    {
        self.elements().for_each(f)
    }

    /// Returns the raw stringtable. Elements in a `PrimitiveBlock` do not store strings
    /// themselves; instead, they just store indices to the stringtable. By convention, the
    /// contained strings are UTF-8 encoded but it is not safe to assume that (use
    /// `std::str::from_utf8`).
    pub fn raw_stringtable(&self) -> &[Vec<u8>] {
        self.block.stringtable.s.as_slice()
    }
}

/// A `PrimitiveGroup` contains a sequence of elements of one type.
#[derive(Clone, Debug)]
pub struct PrimitiveGroup<'a> {
    block: &'a osmformat::PrimitiveBlock,
    group: &'a osmformat::PrimitiveGroup,
}

impl<'a> PrimitiveGroup<'a> {
    fn new(
        block: &'a osmformat::PrimitiveBlock,
        group: &'a osmformat::PrimitiveGroup,
    ) -> PrimitiveGroup<'a> {
        PrimitiveGroup { block, group }
    }

    /// Returns an iterator over the nodes in this group.
    pub fn nodes(&self) -> impl Iterator<Item = Node<'a>> {
        self.group
            .nodes
            .iter()
            .map(|n| Node::new(self.block, n.into()))
    }

    pub fn dense_nodes(&self) -> DenseNodeIter<'a> {
        DenseNodeIter::new(self.block, self.group.dense.get_or_default())
    }

    /// Returns an iterator over the ways in this group.
    pub fn ways(&self) -> impl Iterator<Item = Way<'a>> {
        self.group.ways.iter().map(|w| Way::new(self.block, w))
    }

    /// Returns an iterator over the relations in this group.
    pub fn relations(&self) -> impl Iterator<Item = Relation<'a>> {
        self.group
            .relations
            .iter()
            .map(|r| Relation::new(self.block, r))
    }

    pub fn elements(&self) -> impl Iterator<Item = Element<'a>> {
        self.nodes()
            .map(Element::from)
            .chain(self.dense_nodes().map(Element::from))
            .chain(self.ways().map(Element::from))
            .chain(self.relations().map(Element::from))
    }
}

pub(crate) fn str_from_stringtable(
    block: &osmformat::PrimitiveBlock,
    index: usize,
) -> Result<&str> {
    if let Some(vec) = block.stringtable.s.get(index) {
        std::str::from_utf8(vec)
            .map_err(|e| new_error(ErrorKind::StringtableUtf8 { err: e, index }))
    } else {
        Err(new_error(ErrorKind::StringtableIndexOutOfBounds { index }))
    }
}

/// Construct a key-value tuple from key/value indexes, using the stringtable from a block.
pub(crate) fn get_stringtable_key_value(
    block: &osmformat::PrimitiveBlock,
    key_index: Option<usize>,
    value_index: Option<usize>,
) -> Option<(&str, &str)> {
    match (key_index, value_index) {
        (Some(key_index), Some(val_index)) => {
            let k_res = str_from_stringtable(block, key_index);
            let v_res = str_from_stringtable(block, val_index);
            if let (Ok(k), Ok(v)) = (k_res, v_res) {
                Some((k, v))
            } else {
                None
            }
        }
        _ => None,
    }
}
