//! High level reader interface

use blob::{BlobDecode, BlobReader};
use dense::DenseNode;
use elements::{Node, Way, Relation};
use errors::*;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;


/// A reader for PBF files that gives access to the stored elements: nodes, ways and relations.
pub struct ElementReader<R: Read> {
    blob_iter: BlobReader<R>,
}

impl<R: Read> ElementReader<R> {
    /// Creates a new `ElementReader`.
    /// 
    /// # Example
    /// ```
    /// use osmpbf::*;
    /// 
    /// # fn foo() -> Result<()> {
    /// let f = std::fs::File::open("tests/test.osm.pbf")?;
    /// let buf_reader = std::io::BufReader::new(f);
    /// 
    /// let reader = ElementReader::new(buf_reader);
    /// 
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(reader: R) -> ElementReader<R> {
        ElementReader {
            blob_iter: BlobReader::new(reader),
        }
    }

    /// Decodes the PBF structure sequentially and calls the given closure on each element.
    /// Consider using `par_map_reduce` instead if you need better performance.
    /// 
    /// # Errors
    /// Returns the first Error encountered while parsing the PBF structure.
    /// 
    /// # Example
    /// ```
    /// use osmpbf::*;
    /// 
    /// # fn foo() -> Result<()> {
    /// let reader = ElementReader::from_path("tests/test.osm.pbf")?;
    /// let mut ways = 0_u64;
    ///
    /// // Increment the counter by one for each way.
    /// reader.for_each(|element| {
    ///     if let Element::Way(_) = element {
    ///         ways += 1;
    ///     }
    /// })?;
    /// 
    /// println!("Number of ways: {}", ways);
    /// 
    /// # Ok(())
    /// # }
    /// ```
    pub fn for_each<F>(self, mut f: F) -> Result<()>
        where F: for<'a> FnMut(Element<'a>) {

        let blobs = self.blob_iter.collect::<Result<Vec<_>>>()?;

        //TODO do something useful with header blocks
        for blob in &blobs {
            match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) | Ok(BlobDecode::Unknown(_)) => {},
                Ok(BlobDecode::OsmData(block)) => {
                    for group in block.groups() {
                        group.nodes().for_each(|dnode| f(Element::Node(dnode)));
                        group.dense_nodes().for_each(|node| f(Element::DenseNode(node)));
                        group.ways().for_each(|way| f(Element::Way(way)));
                        group.relations().for_each(|relation| f(Element::Relation(relation)));
                    }
                },
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    /// Parallel map/reduce. Decodes the PBF structure in parallel, calls the closure `map_op` on
    /// each element and then reduces the number of results to one item with the closure
    /// `reduce_op`. Similarly to the `init` argument in the `fold` method on iterators, the
    /// `identity` closure should produce an identity value that is inserted into `reduce_op` when
    /// necessary. The number of times that this identity value is inserted should not alter the
    /// result.
    /// 
    /// # Errors
    /// Returns the first Error encountered while parsing the PBF structure.
    /// 
    /// # Example
    /// ```
    /// use osmpbf::*;
    /// 
    /// # fn foo() -> Result<()> {
    /// let reader = ElementReader::from_path("tests/test.osm.pbf")?;
    ///
    /// // Count the ways
    /// let ways = reader.par_map_reduce(
    ///     |element| {
    ///         match element {
    ///             Element::Way(_) => 1,
    ///             _ => 0,
    ///         }
    ///     },
    ///     || 0_u64,      // Zero is the identity value for addition
    ///     |a, b| a + b   // Sum the partial results
    /// )?;
    /// 
    /// println!("Number of ways: {}", ways);
    /// # Ok(())
    /// # }
    /// ```
    pub fn par_map_reduce<MP, RD, ID, T>(self, map_op: MP, identity: ID, reduce_op: RD) -> Result<T>
        where MP: for<'a> Fn(Element<'a>) -> T + Sync + Send,
              RD: Fn(T, T) -> T + Sync + Send,
              ID: Fn() -> T + Sync + Send,
              T: Send,
    {
        let blobs = self.blob_iter.collect::<Result<Vec<_>>>()?;

        blobs.into_par_iter().map(|blob| {
            match blob.decode() {
                Ok(BlobDecode::OsmHeader(_)) | Ok(BlobDecode::Unknown(_)) => {
                    Ok(identity())
                },
                Ok(BlobDecode::OsmData(block)) => {
                    let dnodes = block.groups()
                         .flat_map(|g| g.dense_nodes())
                         .map(|dn| map_op(Element::DenseNode(dn)));
                    let nodes = block.groups()
                         .flat_map(|g| g.nodes())
                         .map(|n| map_op(Element::Node(n)));
                    let ways = block.groups()
                         .flat_map(|g| g.ways())
                         .map(|w| map_op(Element::Way(w)));
                    let rels = block.groups()
                         .flat_map(|g| g.relations())
                         .map(|r| map_op(Element::Relation(r)));
                
                    Ok(dnodes.chain(nodes)
                        .chain(ways)
                        .chain(rels)
                        .fold(identity(), |a, b| reduce_op(a, b)))
                },
                Err(e) => Err(e),
            }
        }).reduce(|| Ok(identity()), |a, b| {
            match (a, b) {
                (Ok(x), Ok(y)) => Ok(reduce_op(x, y)),
                (x, y) => x.and(y),
            }
        })
    }
}

impl ElementReader<BufReader<File>> {
    /// Tries to open the file at the given path and constructs an `ElementReader` from this.
    /// 
    /// # Errors
    /// Returns the same errors that `std::fs::File::open` returns.
    /// 
    /// # Example
    /// ```
    /// use osmpbf::*;
    /// 
    /// # fn foo() -> Result<()> {
    /// let reader = ElementReader::from_path("tests/test.osm.pbf")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self>
    {
        let f = File::open(path)?;
        let reader = BufReader::new(f);

        Ok(ElementReader {
            blob_iter: BlobReader::new(reader),
        })
    }
}

/// An enum with the OSM core elements: nodes, ways and relations.
pub enum Element<'a> {
    /// A node. Also, see `DenseNode`.
    Node(Node<'a>),

    /// Just like `Node`, but with a different representation in memory. This distinction is
    /// usually not important but is not abstracted away to avoid copying. So, if you want to match
    /// `Node`, you also likely want to match `DenseNode`.
    DenseNode(DenseNode<'a>),

    /// A way.
    Way(Way<'a>),

    /// A relation.
    Relation(Relation<'a>),
}
