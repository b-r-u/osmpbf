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

    /// Returns an iterator over the elements in this `PrimitiveBlock`.
    pub fn elements(&self) -> BlockElementsIter {
        BlockElementsIter::new(&self.block)
    }

    /// Returns an iterator over the groups in this `PrimitiveBlock`.
    pub fn groups(&self) -> GroupIter {
        GroupIter::new(&self.block)
    }

    /// Calls the given closure on each element.
    pub fn for_each_element<F>(&self, mut f: F)
    where
        F: for<'a> FnMut(Element<'a>),
    {
        for group in self.groups() {
            for node in group.nodes() {
                f(Element::Node(node))
            }
            for dnode in group.dense_nodes() {
                f(Element::DenseNode(dnode))
            }
            for way in group.ways() {
                f(Element::Way(way));
            }
            for relation in group.relations() {
                f(Element::Relation(relation));
            }
        }
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
    pub fn nodes(&self) -> GroupNodeIter<'a> {
        GroupNodeIter::new(self.block, self.group)
    }

    /// Returns an iterator over the dense nodes in this group.
    pub fn dense_nodes(&self) -> DenseNodeIter<'a> {
        DenseNodeIter::new(self.block, self.group.dense.get_or_default())
    }

    /// Returns an iterator over the ways in this group.
    pub fn ways(&self) -> GroupWayIter<'a> {
        GroupWayIter::new(self.block, self.group)
    }

    /// Returns an iterator over the relations in this group.
    pub fn relations(&self) -> GroupRelationIter<'a> {
        GroupRelationIter::new(self.block, self.group)
    }
}

/// An iterator over the elements in a [`PrimitiveGroup`].
#[derive(Clone, Debug)]
pub struct BlockElementsIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    state: ElementsIterState,
    groups: std::slice::Iter<'a, osmformat::PrimitiveGroup>,
    dense_nodes: DenseNodeIter<'a>,
    nodes: std::slice::Iter<'a, osmformat::Node>,
    ways: std::slice::Iter<'a, osmformat::Way>,
    relations: std::slice::Iter<'a, osmformat::Relation>,
}

#[derive(Copy, Clone, Debug)]
enum ElementsIterState {
    Group,
    DenseNode,
    Node,
    Way,
    Relation,
}

impl<'a> BlockElementsIter<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock) -> BlockElementsIter<'a> {
        BlockElementsIter {
            block,
            state: ElementsIterState::Group,
            groups: block.primitivegroup.iter(),
            dense_nodes: DenseNodeIter::empty(block),
            nodes: [].iter(),
            ways: [].iter(),
            relations: [].iter(),
        }
    }

    /// Performs an internal iteration step. Returns [`None`] until there is a value for the iterator to
    /// return. Returns [`Some(None)`] to end the iteration.
    #[inline]
    #[allow(clippy::option_option)]
    fn step(&mut self) -> Option<Option<Element<'a>>> {
        match self.state {
            ElementsIterState::Group => match self.groups.next() {
                Some(group) => {
                    self.state = ElementsIterState::DenseNode;
                    self.dense_nodes = DenseNodeIter::new(self.block, group.dense.get_or_default());
                    self.nodes = group.nodes.iter();
                    self.ways = group.ways.iter();
                    self.relations = group.relations.iter();
                    None
                }
                None => Some(None),
            },
            ElementsIterState::DenseNode => match self.dense_nodes.next() {
                Some(dense_node) => Some(Some(Element::DenseNode(dense_node))),
                None => {
                    self.state = ElementsIterState::Node;
                    None
                }
            },
            ElementsIterState::Node => match self.nodes.next() {
                Some(node) => Some(Some(Element::Node(Node::new(self.block, node)))),
                None => {
                    self.state = ElementsIterState::Way;
                    None
                }
            },
            ElementsIterState::Way => match self.ways.next() {
                Some(way) => Some(Some(Element::Way(Way::new(self.block, way)))),
                None => {
                    self.state = ElementsIterState::Relation;
                    None
                }
            },
            ElementsIterState::Relation => match self.relations.next() {
                Some(rel) => Some(Some(Element::Relation(Relation::new(self.block, rel)))),
                None => {
                    self.state = ElementsIterState::Group;
                    None
                }
            },
        }
    }
}

impl<'a> Iterator for BlockElementsIter<'a> {
    type Item = Element<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(element) = self.step() {
                return element;
            }
        }
    }
}

/// An iterator over the groups in a [`PrimitiveBlock`].
#[derive(Clone, Debug)]
pub struct GroupIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    groups: std::slice::Iter<'a, osmformat::PrimitiveGroup>,
}

impl<'a> GroupIter<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock) -> GroupIter<'a> {
        GroupIter {
            block,
            groups: block.primitivegroup.iter(),
        }
    }
}

impl<'a> Iterator for GroupIter<'a> {
    type Item = PrimitiveGroup<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.groups.next() {
            Some(g) => Some(PrimitiveGroup::new(self.block, g)),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.groups.size_hint()
    }
}

impl<'a> ExactSizeIterator for GroupIter<'a> {}

/// An iterator over the nodes in a [`PrimitiveGroup`].
#[derive(Clone, Debug)]
pub struct GroupNodeIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    nodes: std::slice::Iter<'a, osmformat::Node>,
}

impl<'a> GroupNodeIter<'a> {
    fn new(
        block: &'a osmformat::PrimitiveBlock,
        group: &'a osmformat::PrimitiveGroup,
    ) -> GroupNodeIter<'a> {
        GroupNodeIter {
            block,
            nodes: group.nodes.iter(),
        }
    }
}

impl<'a> Iterator for GroupNodeIter<'a> {
    type Item = Node<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nodes.next() {
            Some(n) => Some(Node::new(self.block, n)),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.nodes.size_hint()
    }
}

impl<'a> ExactSizeIterator for GroupNodeIter<'a> {}

/// An iterator over the ways in a [`PrimitiveGroup`].
#[derive(Clone, Debug)]
pub struct GroupWayIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    ways: std::slice::Iter<'a, osmformat::Way>,
}

impl<'a> GroupWayIter<'a> {
    fn new(
        block: &'a osmformat::PrimitiveBlock,
        group: &'a osmformat::PrimitiveGroup,
    ) -> GroupWayIter<'a> {
        GroupWayIter {
            block,
            ways: group.ways.iter(),
        }
    }
}

impl<'a> Iterator for GroupWayIter<'a> {
    type Item = Way<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.ways.next() {
            Some(way) => Some(Way::new(self.block, way)),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ways.size_hint()
    }
}

impl<'a> ExactSizeIterator for GroupWayIter<'a> {}

/// An iterator over the relations in a [`PrimitiveGroup`].
#[derive(Clone, Debug)]
pub struct GroupRelationIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    rels: std::slice::Iter<'a, osmformat::Relation>,
}

impl<'a> GroupRelationIter<'a> {
    fn new(
        block: &'a osmformat::PrimitiveBlock,
        group: &'a osmformat::PrimitiveGroup,
    ) -> GroupRelationIter<'a> {
        GroupRelationIter {
            block,
            rels: group.relations.iter(),
        }
    }
}

impl<'a> Iterator for GroupRelationIter<'a> {
    type Item = Relation<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rels.next() {
            Some(rel) => Some(Relation::new(self.block, rel)),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rels.size_hint()
    }
}

impl<'a> ExactSizeIterator for GroupRelationIter<'a> {}

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
