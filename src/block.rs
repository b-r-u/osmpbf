//! `HeaderBlock`, `PrimitiveBlock` and `PrimitiveGroup`s

use dense::DenseNodeIter;
use elements::{Node, Way, Relation};
use errors::*;
use proto::osmformat;
use std;


/// A `HeaderBlock`. It contains metadata about following `PrimitiveBlock`s.
pub struct HeaderBlock {
    header: osmformat::HeaderBlock,
}

impl HeaderBlock {
    pub(crate) fn new(header: osmformat::HeaderBlock) -> HeaderBlock {
        HeaderBlock { header: header }
    }

    /// Returns a list of required features that a parser needs to implement to parse the following
    /// `PrimitiveBlock`s.
    pub fn required_features(&self) -> &[String] {
        self.header.get_required_features()
    }

    /// Returns a list of optional features that a parser can choose to ignore.
    pub fn optional_features(&self) -> &[String] {
        self.header.get_optional_features()
    }
}

/// A `PrimitiveBlock`. It contains a sequence of groups.
pub struct PrimitiveBlock {
    block: osmformat::PrimitiveBlock,
}

impl PrimitiveBlock {
    pub(crate) fn new(block: osmformat::PrimitiveBlock) -> PrimitiveBlock {
        PrimitiveBlock { block: block }
    }

    /// Returns an iterator over the groups in this `PrimitiveBlock`.
    pub fn groups(&self) -> GroupIter {
        GroupIter::new(&self.block)
    }
}

/// A `PrimitiveGroup` contains a sequence of elements of one type.
pub struct PrimitiveGroup<'a> {
    block: &'a osmformat::PrimitiveBlock,
    group: &'a osmformat::PrimitiveGroup,
}

impl<'a> PrimitiveGroup<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock,
           group: &'a osmformat::PrimitiveGroup)
          -> PrimitiveGroup<'a> {
        PrimitiveGroup {
            block: block,
            group: group,
        }
    }

    /// Returns an iterator over the nodes in this group.
    pub fn nodes(&self) -> GroupNodeIter<'a> {
        GroupNodeIter::new(self.block, self.group)
    }

    /// Returns an iterator over the dense nodes in this group.
    pub fn dense_nodes(&self) -> DenseNodeIter<'a> {
        DenseNodeIter::new(self.block, self.group.get_dense())
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

/// An iterator over the groups in a `PrimitiveBlock`.
pub struct GroupIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    groups: std::slice::Iter<'a, osmformat::PrimitiveGroup>,
}

impl<'a> GroupIter<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock) -> GroupIter<'a> {
        GroupIter {
            block: block,
            groups: block.get_primitivegroup().iter(),
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

/// An iterator over the nodes in a `Group`.
pub struct GroupNodeIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    nodes: std::slice::Iter<'a, osmformat::Node>,
}

impl<'a> GroupNodeIter<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock,
           group: &'a osmformat::PrimitiveGroup)
          -> GroupNodeIter<'a> {
        GroupNodeIter {
            block: block,
            nodes: group.get_nodes().iter(),
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

/// An iterator over the ways in a `Group`.
pub struct GroupWayIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    ways: std::slice::Iter<'a, osmformat::Way>,
}

impl<'a> GroupWayIter<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock,
           group: &'a osmformat::PrimitiveGroup)
          -> GroupWayIter<'a> {
        GroupWayIter {
            block: block,
            ways: group.get_ways().iter(),
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

/// An iterator over the relations in a `Group`.
pub struct GroupRelationIter<'a> {
    block: &'a osmformat::PrimitiveBlock,
    rels: std::slice::Iter<'a, osmformat::Relation>,
}

impl<'a> GroupRelationIter<'a> {
    fn new(block: &'a osmformat::PrimitiveBlock,
           group: &'a osmformat::PrimitiveGroup)
          -> GroupRelationIter<'a> {
        GroupRelationIter {
            block: block,
            rels: group.get_relations().iter(),
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

pub(crate) fn str_from_stringtable(block: &osmformat::PrimitiveBlock, index: usize) -> Result<&str> {
    if let Some(vec) = block.get_stringtable().get_s().get(index) {
        std::str::from_utf8(vec)
            .chain_err(|| "failed to decode string from string table")
    } else {
        Err(ErrorKind::StringtableIndexOutOfBounds(index).into())
    }
}
