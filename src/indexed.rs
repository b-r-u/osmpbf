//! Speed up searches by using an index

use crate::error::Result;
use crate::{BlobReader, BlobType, ByteOffset, Element, PrimitiveBlock, Way};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek};
use std::ops::RangeInclusive;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleBlobType {
    Header,
    Primitive,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ElementsAvailable {
    Yes,
    No,
    Unknown,
}

/// Returns true if the given set contains at least one value that is inside the given range.
fn range_included(range: RangeInclusive<i64>, node_ids: &BTreeSet<i64>) -> bool {
    node_ids.range(range).next().is_some()
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RangeIncluded {
    Yes(RangeInclusive<i64>),
    No,
    Unknown,
}

/// Stores the minimum and maximum id of every element type.
#[derive(Debug)]
pub struct IdRanges {
    node_ids: Option<RangeInclusive<i64>>,
    way_ids: Option<RangeInclusive<i64>>,
    //TODO actually use this field
    #[allow(dead_code)]
    relation_ids: Option<RangeInclusive<i64>>,
}

/// A part of the index that stores information about a specific blob.
#[derive(Debug)]
struct BlobInfo {
    offset: ByteOffset,
    blob_type: SimpleBlobType,
    id_ranges: Option<IdRanges>,
}

impl BlobInfo {
    /// Is there at least one node in this blob?
    fn nodes_available(&self) -> ElementsAvailable {
        match self.id_ranges {
            Some(IdRanges {
                node_ids: Some(_), ..
            }) => ElementsAvailable::Yes,
            Some(IdRanges { node_ids: None, .. }) => ElementsAvailable::No,
            None => ElementsAvailable::Unknown,
        }
    }

    /// Is there at least one way in this blob?
    fn ways_available(&self) -> ElementsAvailable {
        match self.id_ranges {
            Some(IdRanges {
                way_ids: Some(_), ..
            }) => ElementsAvailable::Yes,
            Some(IdRanges { way_ids: None, .. }) => ElementsAvailable::No,
            None => ElementsAvailable::Unknown,
        }
    }

    /*
    /// Is there at least one relation in this blob?
    fn relations_available(&self) -> ElementsAvailable {
        match self.id_ranges {
            Some(IdRanges {relation_ids: Some(_), ..}) => ElementsAvailable::Yes,
            Some(IdRanges {relation_ids: None, ..}) => ElementsAvailable::No,
            None => ElementsAvailable::Unknown,
        }
    }
    */

    /// Compute if the range of node IDs of this blob (min and max ID value) is included in the
    /// given set of IDs with at least one ID inside of this range.
    fn node_range_included(&self, node_ids: &BTreeSet<i64>) -> RangeIncluded {
        match self.id_ranges.as_ref() {
            None => RangeIncluded::Unknown,
            Some(IdRanges { node_ids: None, .. }) => RangeIncluded::No,
            Some(IdRanges {
                node_ids: Some(range),
                ..
            }) => {
                if range_included(range.clone(), node_ids) {
                    RangeIncluded::Yes(range.clone())
                } else {
                    RangeIncluded::No
                }
            }
        }
    }
}

/// Allows filtering elements and iterating over their dependencies.
/// It chooses an efficient method for navigating the PBF structure to achieve this in reasonable
/// time and with reasonable memory.
pub struct IndexedReader<R: Read + Seek + Send> {
    reader: BlobReader<R>,
    index: Vec<BlobInfo>,
}

impl<R: Read + Seek + Send> IndexedReader<R> {
    /// Creates a new `IndexedReader`.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let f = std::fs::File::open("tests/test.osm.pbf")?;
    /// let buf_reader = std::io::BufReader::new(f);
    ///
    /// let reader = IndexedReader::new(buf_reader)?;
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn new(reader: R) -> Result<Self> {
        let reader = BlobReader::new_seekable(reader)?;
        Ok(Self {
            reader,
            index: vec![],
        })
    }

    /// Initializes the index of the PBF structure without decompressing the blobs.
    /// You do not need to call this method explicitly as the other methods already take care of
    /// it.
    pub fn create_index(&mut self) -> Result<()> {
        if !self.index.is_empty() {
            // Index is already present -> Do nothing
            return Ok(());
        }

        // Seek to the beginning of the reader.
        self.reader.seek(ByteOffset(0))?;

        while let Some(result) = self.reader.next_header_skip_blob() {
            let (header, offset) = result?;
            // Reader is seekable, so offset should be Some(ByteOffset)
            let offset = offset.unwrap();
            let blob_type = match header.blob_type() {
                BlobType::OsmHeader => SimpleBlobType::Header,
                BlobType::OsmData => SimpleBlobType::Primitive,
                BlobType::Unknown(_) => SimpleBlobType::Unknown,
            };

            self.index.push(BlobInfo {
                offset,
                blob_type,
                id_ranges: None,
            });
        }

        Ok(())
    }

    /// Check element IDs of this block. Record min and max for every node, way and relation.
    fn update_element_id_ranges(info: &mut BlobInfo, block: &PrimitiveBlock) {
        if info.id_ranges.is_some() {
            // Ranges are already present -> Do nothing
            return;
        }

        let mut min_node_id: Option<i64> = None;
        let mut max_node_id: Option<i64> = None;
        let mut min_way_id: Option<i64> = None;
        let mut max_way_id: Option<i64> = None;
        let mut min_relation_id: Option<i64> = None;
        let mut max_relation_id: Option<i64> = None;

        // Check each primitive group
        for group in block.groups() {
            let check_min_max = |id, min_id: &mut Option<i64>, max_id: &mut Option<i64>| {
                *min_id = Some(min_id.map_or(id, |x| x.min(id)));
                *max_id = Some(max_id.map_or(id, |x| x.max(id)));
            };

            for node in group.nodes() {
                check_min_max(node.id(), &mut min_node_id, &mut max_node_id);
            }
            for node in group.dense_nodes() {
                check_min_max(node.id, &mut min_node_id, &mut max_node_id);
            }
            for way in group.ways() {
                check_min_max(way.id(), &mut min_way_id, &mut max_way_id);
            }
            for relation in group.relations() {
                check_min_max(relation.id(), &mut min_relation_id, &mut max_relation_id);
            }
        }

        let to_range = |min_id, max_id| -> Option<RangeInclusive<i64>> {
            if let (Some(min), Some(max)) = (min_id, max_id) {
                Some(RangeInclusive::new(min, max))
            } else {
                None
            }
        };

        info.id_ranges = Some(IdRanges {
            node_ids: to_range(min_node_id, max_node_id),
            way_ids: to_range(min_way_id, max_way_id),
            relation_ids: to_range(min_relation_id, max_relation_id),
        });
    }

    /// Filter ways using a closure and return matching ways and their dependent nodes
    /// ([`Node`](crate::elements::Node)s and [`DenseNode`](crate::dense::DenseNode)s)
    /// in another closure.
    /// This method also creates a lightweight in-memory index that speeds up future invocations of
    /// this or any other method of `IndexedReader`.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let mut reader = IndexedReader::from_path("tests/test.osm.pbf")?;
    /// let mut ways = 0;
    /// let mut nodes = 0;
    ///
    /// // Filter all ways that are buildings and count their nodes.
    /// reader.read_ways_and_deps(
    ///     |way| {
    ///         // Filter ways. Return true if tags contain "building": "yes".
    ///         way.tags().any(|key_value| key_value == ("building", "yes"))
    ///     },
    ///     |element| {
    ///         // Increment counter
    ///         match element {
    ///             Element::Way(way) => ways += 1,
    ///             Element::Node(node) => nodes += 1,
    ///             Element::DenseNode(dense_node) => nodes += 1,
    ///             Element::Relation(_) => (), // should not occur
    ///         }
    ///     },
    /// )?;
    ///
    /// println!("ways:  {ways}\nnodes: {nodes}");
    ///
    /// # assert_eq!(ways, 1);
    /// # assert_eq!(nodes, 3);
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn read_ways_and_deps<F, E>(&mut self, mut filter: F, mut element_callback: E) -> Result<()>
    where
        F: for<'a> FnMut(&Way<'a>) -> bool,
        E: for<'a> FnMut(&Element<'a>),
    {
        self.create_index()?;

        let mut node_ids: BTreeSet<i64> = BTreeSet::new();

        // First pass:
        //   * Filter ways and store their dependencies as node IDs
        for info in &mut self.index {
            //TODO do something useful with header blocks
            if info.blob_type == SimpleBlobType::Primitive
                && info.ways_available() != ElementsAvailable::No
            {
                let block = self
                    .reader
                    .blob_from_offset(info.offset)?
                    .to_primitiveblock()?;
                Self::update_element_id_ranges(info, &block);

                for group in block.groups() {
                    // filter ways and record node IDs
                    for way in group.ways() {
                        if filter(&way) {
                            let refs = way.refs();

                            node_ids.extend(refs);

                            // Return way
                            element_callback(&Element::Way(way));
                        }
                    }
                }
            }
        }

        // Second pass:
        //   * Iterate only over blobs that may include the node IDs we're searching for
        for info in &mut self.index {
            if let RangeIncluded::Yes(node_id_range) = info.node_range_included(&node_ids) {
                //TODO Only collect into Vec if range has a reasonable size
                let node_ids: Vec<i64> = node_ids.range(node_id_range).copied().collect();
                let block = self
                    .reader
                    .blob_from_offset(info.offset)?
                    .to_primitiveblock()?;
                for group in block.groups() {
                    for node in group.nodes() {
                        if node_ids.binary_search(&node.id()).is_ok() {
                            // ID found, return node
                            element_callback(&Element::Node(node));
                        }
                    }
                    for node in group.dense_nodes() {
                        if node_ids.binary_search(&node.id).is_ok() {
                            // ID found, return dense node
                            element_callback(&Element::DenseNode(node));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Decodes the PBF structure sequentially and calls the given closure on each node.
    /// This method also creates a lightweight in-memory index that speeds up future invocations of
    /// this or any other method of `IndexedReader`.
    ///
    /// # Errors
    /// Returns the first Error encountered while parsing the PBF structure.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let mut reader = IndexedReader::from_path("tests/test.osm.pbf")?;
    /// let mut nodes = 0;
    ///
    /// reader.for_each_node(
    ///     |element| {
    ///         match element {
    ///             Element::Node(node) => nodes += 1,
    ///             Element::DenseNode(dense_node) => nodes += 1,
    ///             _ => {}
    ///         }
    ///     },
    /// )?;
    ///
    /// println!("nodes: {nodes}");
    ///
    /// # assert_eq!(nodes, 3);
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn for_each_node<F>(&mut self, mut f: F) -> Result<()>
    where
        F: for<'a> FnMut(Element<'a>),
    {
        self.create_index()?;

        for info in &mut self.index {
            // Skip header blobs and blobs where there are certainly no nodes available.
            if info.blob_type == SimpleBlobType::Primitive
                && info.nodes_available() != ElementsAvailable::No
            {
                let block = self
                    .reader
                    .blob_from_offset(info.offset)?
                    .to_primitiveblock()?;
                Self::update_element_id_ranges(info, &block);

                for group in block.groups() {
                    for node in group.nodes() {
                        f(Element::Node(node));
                    }
                    for dense_node in group.dense_nodes() {
                        f(Element::DenseNode(dense_node));
                    }
                }
            }
        }

        Ok(())
    }
}

impl IndexedReader<File> {
    /// Creates a new `IndexedReader` from a given path.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let reader = IndexedReader::from_path("tests/test.osm.pbf")?;
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        //TODO take some more measurements to determine if `BufReader` should be used here
        let f = File::open(path)?;
        Self::new(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_included_set() {
        let mut set = BTreeSet::<i64>::new();
        set.extend(&[1, 2, 6]);

        assert!(!range_included(RangeInclusive::new(0, 0), &set));
        assert!(range_included(RangeInclusive::new(1, 1), &set));
        assert!(range_included(RangeInclusive::new(2, 2), &set));
        assert!(!range_included(RangeInclusive::new(3, 3), &set));
        assert!(!range_included(RangeInclusive::new(3, 5), &set));
        assert!(range_included(RangeInclusive::new(3, 6), &set));
        assert!(range_included(RangeInclusive::new(6, 6), &set));
        assert!(!range_included(RangeInclusive::new(7, 7), &set));
        assert!(range_included(RangeInclusive::new(0, 1), &set));
        assert!(range_included(RangeInclusive::new(6, 7), &set));
        assert!(range_included(RangeInclusive::new(2, 3), &set));
        assert!(range_included(RangeInclusive::new(5, 6), &set));
        assert!(range_included(RangeInclusive::new(5, 8), &set));
        assert!(range_included(RangeInclusive::new(0, 8), &set));
        assert!(range_included(RangeInclusive::new(0, 4), &set));
    }
}
