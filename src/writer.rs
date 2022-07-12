//! Encode and write blobs and blocks

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Write;

use byteorder::WriteBytesExt;
use flate2::{write::ZlibEncoder, Compression};
use protobuf::{Message, MessageField};

use crate::blob::{Blob, BlobType, MAX_BLOB_HEADER_SIZE, MAX_BLOB_MESSAGE_SIZE};
use crate::block::{HeaderBlock, PrimitiveBlock};
use crate::error::{new_blob_error, new_protobuf_error, BlobError, Result};
use crate::proto::{fileformat, osmformat};

/// A writer for PBF files that allows writing blobs.
#[derive(Clone, Debug)]
pub struct BlobWriter<W: Write + Send> {
    writer: W,
}

/// The content type of a blob.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlobEncoding {
    /// Block is stored uncompressed in blob
    Raw,
    /// Block data is stored zlib-compressed with the specified compression level (0-9):
    ///
    /// * 0 - no compression (might actually increase size -> use `Raw` instead)
    /// * 1 - fast
    /// * 6 - A common default value
    /// * 9 - best compression, but slower
    Zlib { level: u32 },
}

impl<W: Write + Send> BlobWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub(crate) fn write_blob_raw(
        &mut self,
        header: fileformat::BlobHeader,
        blob: fileformat::Blob,
    ) -> Result<()> {
        assert_eq!(blob.compute_size() as i64, i64::from(header.datasize()),);
        let header_size: u64 = header.compute_size();

        if header_size >= MAX_BLOB_HEADER_SIZE {
            return Err(new_blob_error(BlobError::HeaderTooBig {
                size: header_size,
            }));
        }

        self.writer
            .write_u32::<byteorder::BigEndian>(header_size as u32)?;
        header
            .write_to_writer(&mut self.writer)
            .map_err(|e| new_protobuf_error(e, "writing blob header"))?;
        blob.write_to_writer(&mut self.writer)
            .map_err(|e| new_protobuf_error(e, "writing blob"))?;

        Ok(())
    }

    pub fn write_blob(&mut self, blob: Blob) -> Result<()> {
        self.write_blob_raw(blob.header, blob.blob)
    }

    /// Create Blob from raw (uncompressed) encoded block data
    fn encode_block_data(block_data: Vec<u8>, encoding: BlobEncoding) -> Result<fileformat::Blob> {
        if block_data.len() as u64 >= MAX_BLOB_MESSAGE_SIZE {
            return Err(new_blob_error(BlobError::MessageTooBig {
                size: block_data.len() as u64,
            }));
        }

        let mut blob = fileformat::Blob::new();
        blob.set_raw_size(block_data.len() as i32);

        match encoding {
            BlobEncoding::Raw => {
                blob.set_raw(block_data);
            }
            BlobEncoding::Zlib { level } => {
                assert!(level < 10);
                let mut encoder = ZlibEncoder::new(vec![], Compression::new(level));
                encoder.write_all(&block_data)?;
                blob.set_zlib_data(encoder.finish()?);
            }
        }

        Ok(blob)
    }

    fn write_block_message<M>(
        &mut self,
        block: M,
        blob_type: BlobType,
        encoding: BlobEncoding,
        error_string: &'static str,
    ) -> Result<()>
    where
        M: protobuf::Message,
    {
        let mut block_data = vec![];
        block
            .write_to_writer(&mut block_data)
            .map_err(|e| new_protobuf_error(e, error_string))?;
        let blob = Self::encode_block_data(block_data, encoding)?;

        let mut header = fileformat::BlobHeader::new();
        header.set_datasize(blob.compute_size() as i32);
        header.set_type(blob_type.as_str().to_string());
        //TODO optionally set indexdata

        self.write_blob_raw(header, blob)
    }

    /// Write a new blob that encodes the given [`HeaderBlock`].
    ///
    /// The first blob of a `*.osm.pbf` file is usually a header block.
    pub fn write_header_block(&mut self, block: HeaderBlock, encoding: BlobEncoding) -> Result<()> {
        self.write_block_message(
            block.header,
            BlobType::OsmHeader,
            encoding,
            "writing header block",
        )
    }

    /// Write a new blob that encodes the given [`PrimitiveBlock`].
    ///
    /// A primitive block may contain nodes, ways and relations.
    pub fn write_primitive_block(
        &mut self,
        block: PrimitiveBlock,
        encoding: BlobEncoding,
    ) -> Result<()> {
        self.write_block_message(
            block.block,
            BlobType::OsmData,
            encoding,
            "writing primitive block",
        )
    }

    //TODO write unknown blob type
}

/// A builder for `PrimitiveBlock`s.
pub struct BlockBuilder<W: Write + Send> {
    blob_writer: BlobWriter<W>,
    block: osmformat::PrimitiveBlock,
    string_map: HashMap<Vec<u8>, usize>,
}

impl<W: Write + Send> BlockBuilder<W> {
    pub fn new(blob_writer: BlobWriter<W>) -> Self {
        let mut block = osmformat::PrimitiveBlock::new();
        let mut st = osmformat::StringTable::new();
        // first element should be blank
        st.s = vec![vec![]];
        block.stringtable = MessageField::some(st);
        Self {
            blob_writer,
            block,
            string_map: HashMap::new(),
        }
    }

    /// Given a string (`Vec<u8>`) return its index from the string table.
    /// If the string is not yet included, it will be inserted.
    pub(crate) fn add_string_table_entry(&mut self, entry: Vec<u8>) -> usize {
        match self.string_map.entry(entry.clone()) {
            Entry::Occupied(occ) => *occ.get(),
            Entry::Vacant(vac) => {
                let st = &mut self.block.stringtable.as_mut().unwrap().s;
                st.push(entry);
                *vac.insert(st.len() - 1)
            }
        }
    }

    pub fn node_group(&mut self) -> NodeGroupBuilder<W> {
        NodeGroupBuilder::new(self)
    }

    pub fn finish(mut self, encoding: BlobEncoding) -> Result<BlobWriter<W>> {
        self.blob_writer
            .write_primitive_block(PrimitiveBlock::new(self.block), encoding)?;
        Ok(self.blob_writer)
    }
}

pub struct NodeGroupBuilder<'a, W: Write + Send> {
    pub(crate) block_builder: &'a mut BlockBuilder<W>,
    pub(crate) group: osmformat::PrimitiveGroup,
}

impl<'a, W: Write + Send> NodeGroupBuilder<'a, W> {
    pub(crate) fn new(block_builder: &'a mut BlockBuilder<W>) -> Self {
        Self {
            block_builder,
            group: osmformat::PrimitiveGroup::new(),
        }
    }

    pub fn node_builder<'b>(&'b mut self) -> NodeBuilder<'b, 'a, W> {
        NodeBuilder {
            node_group_builder: self,
            node: osmformat::Node::new(),
        }
    }

    pub fn finish(self) {
        self.block_builder.block.primitivegroup.push(self.group);
    }
}

pub struct NodeBuilder<'a, 'b, W: Write + Send> {
    node_group_builder: &'a mut NodeGroupBuilder<'b, W>,
    node: osmformat::Node,
}

impl<'a, 'b, W: Write + Send> NodeBuilder<'a, 'b, W> {
    pub fn id(mut self, id: i64) -> Self {
        self.node.set_id(id);
        self
    }

    pub fn latlon(mut self, lat: f64, lon: f64) -> Self {
        self.node.set_lat((lat * 1e-7).round() as i64);
        self.node.set_lon((lon * 1e-7).round() as i64);
        self
    }

    pub fn add_tag<K, V>(mut self, key: K, val: V) -> Self
    where
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let block = &mut self.node_group_builder.block_builder;
        self.node
            .keys
            .push(block.add_string_table_entry(key.into()) as u32);
        self.node
            .vals
            .push(block.add_string_table_entry(val.into()) as u32);
        self
    }

    //TODO implement setting Info

    pub fn finish(self) {
        self.node_group_builder.group.nodes.push(self.node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::osmformat;

    #[test]
    fn test_blob_writer() {
        let buf = vec![];
        let mut w = BlobWriter::new(buf);

        {
            let block = HeaderBlock::new(osmformat::HeaderBlock::new());
            w.write_header_block(block, BlobEncoding::Zlib { level: 6 })
                .unwrap();
        }

        {
            let mut block = osmformat::PrimitiveBlock::new();
            block.stringtable = MessageField::some(osmformat::StringTable::new());
            block.primitivegroup = Vec::new();
            let block = PrimitiveBlock::new(block);

            w.write_primitive_block(block, BlobEncoding::Zlib { level: 6 })
                .unwrap();
        }
    }

    #[test]
    fn test_block_builder() {
        let mut buf = vec![];
        let w = BlobWriter::new(&mut buf);
        let mut block_builder = BlockBuilder::new(w);
        assert_eq!(block_builder.add_string_table_entry("abc".into()), 1);
        assert_eq!(block_builder.add_string_table_entry("xyz".into()), 2);
        assert_eq!(block_builder.add_string_table_entry("abc".into()), 1);
        assert_eq!(block_builder.add_string_table_entry("123".into()), 3);
        block_builder.finish(BlobEncoding::Raw).unwrap();
    }

    #[test]
    fn test_node_builder() {
        let mut buf = vec![];
        let w = BlobWriter::new(&mut buf);
        let mut block_builder = BlockBuilder::new(w);
        {
            let mut group = block_builder.node_group();
            group
                .node_builder()
                .id(12)
                .latlon(52.4, 13.05)
                .add_tag("name", "Potsdam")
                .add_tag("place", "city")
                .finish();
            group
                .node_builder()
                .id(13)
                .latlon(52.51, 13.35)
                .add_tag("name", "Berlin")
                .add_tag("place", "city")
                .add_tag("capital", "yes")
                .finish();
            group.finish();
        }
        block_builder.finish(BlobEncoding::Raw).unwrap();
    }
}
