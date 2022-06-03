use std::io::Write;

use byteorder::WriteBytesExt;
use protobuf::Message;

#[cfg(feature = "system-libz")]
use flate2::{write::ZlibEncoder, Compression};

use crate::blob::{Blob, BlobType, MAX_BLOB_HEADER_SIZE, MAX_BLOB_MESSAGE_SIZE};
use crate::block::{HeaderBlock, PrimitiveBlock};
use crate::error::{new_blob_error, new_protobuf_error, BlobError, Result};
use crate::proto::fileformat;

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

        //TODO >= or = ?
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
        //TODO >= or = ?
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
        error_string: &'static str,
    ) -> Result<()>
    where
        M: protobuf::Message,
    {
        let mut block_data = vec![];
        block
            .write_to_writer(&mut block_data)
            .map_err(|e| new_protobuf_error(e, error_string))?;
        let blob = Self::encode_block_data(block_data, BlobEncoding::Zlib { level: 6 })?;

        let mut header = fileformat::BlobHeader::new();
        header.set_datasize(blob.compute_size() as i32);
        header.set_field_type(blob_type.as_str().to_string());
        //TODO optionally set indexdata

        self.write_blob_raw(header, blob)
    }

    pub fn write_header_block(&mut self, block: HeaderBlock) -> Result<()> {
        self.write_block_message(block.header, BlobType::OsmHeader, "writing header block")
    }

    pub fn write_primitive_block(&mut self, block: PrimitiveBlock) -> Result<()> {
        self.write_block_message(block.block, BlobType::OsmData, "writing primitive block")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::osmformat;

    #[test]
    fn test_writer() {
        let buf = vec![];
        let mut w = BlobWriter::new(buf);

        {
            let block = HeaderBlock::new(osmformat::HeaderBlock::new());
            w.write_header_block(block).unwrap();
        }

        {
            let mut block = osmformat::PrimitiveBlock::new();
            block.set_stringtable(osmformat::StringTable::new());
            block.set_primitivegroup(Vec::new().into());
            let block = PrimitiveBlock::new(block);

            w.write_primitive_block(block).unwrap();
        }
    }
}
