//! Speed up searches by using an index

use error::Result;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek};
use std::ops::RangeInclusive;
use std::path::Path;
use {BlobReader, BlobType, ByteOffset, Element, Way};

/// Stores the minimum and maximum id of every element type.
#[derive(Debug)]
pub struct IdRanges {
    node_ids: Option<RangeInclusive<i64>>,
    way_ids: Option<RangeInclusive<i64>>,
    relation_ids: Option<RangeInclusive<i64>>,
}

/// Returns true if the given set contains at least one value that is inside the given range.
fn range_included(range: RangeInclusive<i64>, node_ids: &BTreeSet<i64>) -> bool {
    node_ids.range(range).next().is_some()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleBlobType {
    Header,
    Primitive,
    Unknown,
}

#[derive(Debug)]
struct BlobInfo {
    offset: ByteOffset,
    blob_type: SimpleBlobType,
    id_ranges: Option<IdRanges>,
}

/// Allows filtering elements and iterating over their dependencies.
/// It chooses an efficient method for navigating the PBF structure to achieve this in reasonable
/// time and with reasonable memory.
pub struct IndexedReader<R: Read + Seek> {
    reader: BlobReader<R>,
    index: Vec<BlobInfo>,
}

impl<R: Read + Seek> IndexedReader<R> {
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

    pub fn create_index(&mut self) -> Result<()> {
        // remove old items
        self.index.clear();

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

    /// Filter ways using a closure and return matching ways and their dependent nodes (`Node`s and
    /// `DenseNode`s) in another closure.
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
    /// println!("ways:  {}\nnodes: {}", ways, nodes);
    ///
    /// # assert_eq!(ways, 1);
    /// # assert_eq!(nodes, 3);
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn read_ways_and_deps<F, E>(
        &mut self,
        mut filter: F,
        mut element_callback: E,
    ) -> Result<()>
    where
        F: for<'a> FnMut(&Way<'a>) -> bool,
        E: for<'a> FnMut(&Element<'a>),
    {
        // Create index
        if self.index.is_empty() {
            self.create_index()?;
        }

        let mut node_ids: BTreeSet<i64> = BTreeSet::new();

        // First pass:
        //   * Filter ways and store their dependencies as node IDs
        //   * Store range of node IDs (min and max value) of each block
        for info in &mut self.index {
            //TODO do something useful with header blocks
            if info.blob_type == SimpleBlobType::Primitive {
                self.reader.seek(info.offset)?;
                let blob = self.reader.next().ok_or_else(|| {
                    ::std::io::Error::new(
                        ::std::io::ErrorKind::UnexpectedEof,
                        "could not read next blob",
                    )
                })??;
                let block = blob.to_primitiveblock()?;
                let mut min_node_id: Option<i64> = None;
                let mut max_node_id: Option<i64> = None;
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

                    // Check node IDs of this block, record min and max

                    let mut check_min_max = |id| {
                        min_node_id = Some(min_node_id.map_or(id, |x| x.min(id)));
                        max_node_id = Some(max_node_id.map_or(id, |x| x.max(id)));
                    };

                    for node in group.nodes() {
                        check_min_max(node.id())
                    }
                    for node in group.dense_nodes() {
                        check_min_max(node.id)
                    }
                }
                if let (Some(min), Some(max)) = (min_node_id, max_node_id) {
                    info.id_ranges = Some(IdRanges {
                        node_ids: Some(RangeInclusive::new(min, max)),
                        way_ids: None,
                        relation_ids: None,
                    });
                }
            }
        }

        // Second pass:
        //   * Iterate only over blobs that may include the node IDs we're searching for
        for info in &mut self.index {
            if info.blob_type == SimpleBlobType::Primitive {
                if let Some(node_id_range) = info.id_ranges.as_ref().and_then(|r| r.node_ids.as_ref()) {
                    if range_included(node_id_range.clone(), &node_ids) {
                        self.reader.seek(info.offset)?;
                        let blob = self.reader.next().ok_or_else(|| {
                            ::std::io::Error::new(
                                ::std::io::ErrorKind::UnexpectedEof,
                                "could not read next blob",
                            )
                        })??;
                        let block = blob.to_primitiveblock()?;
                        for group in block.groups() {
                            for node in group.nodes() {
                                if node_ids.contains(&node.id()) {
                                    // ID found, return node
                                    element_callback(&Element::Node(node));
                                }
                            }
                            for node in group.dense_nodes() {
                                if node_ids.contains(&node.id) {
                                    // ID found, return dense node
                                    element_callback(&Element::DenseNode(node));
                                }
                            }
                        }
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
        set.extend(&[1,2,6]);

        assert_eq!(range_included(RangeInclusive::new(0, 0), &set), false);
        assert_eq!(range_included(RangeInclusive::new(1, 1), &set), true);
        assert_eq!(range_included(RangeInclusive::new(2, 2), &set), true);
        assert_eq!(range_included(RangeInclusive::new(3, 3), &set), false);
        assert_eq!(range_included(RangeInclusive::new(3, 5), &set), false);
        assert_eq!(range_included(RangeInclusive::new(3, 6), &set), true);
        assert_eq!(range_included(RangeInclusive::new(6, 6), &set), true);
        assert_eq!(range_included(RangeInclusive::new(7, 7), &set), false);
        assert_eq!(range_included(RangeInclusive::new(0, 1), &set), true);
        assert_eq!(range_included(RangeInclusive::new(6, 7), &set), true);
        assert_eq!(range_included(RangeInclusive::new(2, 3), &set), true);
        assert_eq!(range_included(RangeInclusive::new(5, 6), &set), true);
        assert_eq!(range_included(RangeInclusive::new(5, 8), &set), true);
        assert_eq!(range_included(RangeInclusive::new(0, 8), &set), true);
        assert_eq!(range_included(RangeInclusive::new(0, 4), &set), true);
    }
}
