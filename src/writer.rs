use std::io::Write;

use byteorder::WriteBytesExt;
use protobuf::Message;

#[cfg(feature = "system-libz")]
use flate2::{write::ZlibEncoder, Compression};

use crate::blob::{BlobType, MAX_BLOB_HEADER_SIZE, MAX_BLOB_MESSAGE_SIZE};
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

    pub fn write_blob(&mut self, btype: BlobType, blob: fileformat::Blob) -> Result<()> {
        let mut header = fileformat::BlobHeader::new();
        header.set_datasize(blob.compute_size() as i32);
        header.set_field_type(btype.as_str().to_string());
        //TODO optionally set indexdata
        self.write_blob_raw(header, blob)
    }

    /// Create Blob from raw (uncompressed) encoded block data
    pub fn encode_block_data(
        block_data: Vec<u8>,
        encoding: BlobEncoding,
    ) -> Result<fileformat::Blob> {
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
            BlobEncoding::Zlib{level} => {
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

    pub fn write_header_block(&mut self, block: HeaderBlock) {
        /*
        let mut header = fileformat::BlobHeader::new();
        let mut block_data = vec![];
        block.header.write_to_writer(&mut block_data);

        //header.set_datasize(blob.compute_size() as i32);
        //header.set_field_type(btype.as_str().to_string());
        */
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer() {
        let buf = vec![];
        let mut w = BlobWriter::new(buf);
        let blob = fileformat::Blob::new();
        w.write_blob(BlobType::OsmHeader, blob).unwrap();
    }
}
