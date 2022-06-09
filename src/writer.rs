//! Encode and write blobs and blocks

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Write;

use byteorder::WriteBytesExt;
use protobuf::Message;

#[cfg(feature = "system-libz")]
use flate2::{write::ZlibEncoder, Compression};

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
#[derive(Clone, Debug, Eq, PartialEq)]
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
        assert_eq!(
            i64::from(blob.compute_size()),
            i64::from(header.get_datasize())
        );
        let header_size = header.compute_size();

        if u64::from(header_size) >= MAX_BLOB_HEADER_SIZE {
            return Err(new_blob_error(BlobError::HeaderTooBig {
                size: u64::from(header_size),
            }));
        }

        self.writer.write_u32::<byteorder::BigEndian>(header_size)?;
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
                if cfg!(feature = "system-libz") {
                    assert!(level < 10);
                    let mut encoder = ZlibEncoder::new(vec![], Compression::new(level));
                    encoder.write_all(&block_data)?;
                    blob.set_zlib_data(encoder.finish()?);
                } else {
                    unimplemented!();
                }
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
        header.set_field_type(blob_type.as_str().to_string());
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
        st.set_s(vec![vec![]].into());
        block.set_stringtable(st);
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
                let st = self.block.mut_stringtable().mut_s();
                st.push(entry);
                *vac.insert(st.len() - 1)
            }
        }
    }

    pub fn finish(mut self, encoding: BlobEncoding) -> Result<BlobWriter<W>> {
        self.blob_writer
            .write_primitive_block(PrimitiveBlock::new(self.block), encoding)?;
        Ok(self.blob_writer)
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
            block.set_stringtable(osmformat::StringTable::new());
            block.set_primitivegroup(Vec::new().into());
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
}
