use crate::error::{new_blob_error, new_protobuf_error};
use crate::proto::fileformat;
use crate::{Blob, BlobError, BlobRange, ByteOffset, MAX_BLOB_HEADER_SIZE};
use byteorder::ReadBytesExt;
use protobuf::Message;
use rayon::prelude::*;
use std::io::{BufReader, Read, Seek, SeekFrom};

/// Sequential stage: raw bytes from file
#[derive(Clone, Debug)]
pub struct RawBlob {
    pub header: fileformat::BlobHeader,
    pub data: Vec<u8>,
}

/// Iterator that reads raw blobs sequentially
#[derive(Debug)]
pub struct RawBlobReader<'a, R: Read> {
    reader: R,
    offset: Option<ByteOffset>,
    last_blob_ok: bool,
    pub blob_ranges: &'a mut Vec<BlobRange>,
}

impl<'a, R: Read + Send> RawBlobReader<'a, R> {
    pub fn new(reader: R, blob_ranges: &'a mut Vec<BlobRange>) -> Self {
        Self {
            reader,
            offset: Some(ByteOffset(0)),
            last_blob_ok: true,
            blob_ranges,
        }
    }

    fn read_blob_header(&mut self) -> Option<crate::Result<(fileformat::BlobHeader, u64)>> {
        let header_size: u64 = match self.reader.read_u32::<byteorder::BigEndian>() {
            Ok(n) => {
                self.offset = self.offset.map(|x| ByteOffset(x.0 + 4));
                u64::from(n)
            }
            Err(e) => {
                self.offset = None;
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => None,
                    _ => {
                        self.last_blob_ok = false;
                        Some(Err(new_blob_error(BlobError::InvalidHeaderSize)))
                    }
                };
            }
        };

        if header_size >= MAX_BLOB_HEADER_SIZE {
            self.last_blob_ok = false;
            return Some(Err(new_blob_error(BlobError::HeaderTooBig {
                size: header_size,
            })));
        }

        let mut reader = self.reader.by_ref().take(header_size);
        let header = match fileformat::BlobHeader::parse_from_reader(&mut reader) {
            Ok(h) => h,
            Err(e) => {
                self.offset = None;
                self.last_blob_ok = false;
                return Some(Err(new_protobuf_error(e, "blob header")));
            }
        };

        self.offset = self.offset.map(|x| ByteOffset(x.0 + header_size));
        Some(Ok((header, header_size)))
    }
}

impl<R: Read + Send> Iterator for RawBlobReader<'_, R> {
    type Item = crate::Result<RawBlob>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.last_blob_ok {
            return None;
        }

        let prev_offset = self.offset;
        let (header, header_size) = match self.read_blob_header()? {
            Ok(h) => h,
            Err(e) => return Some(Err(e)),
        };

        let mut buf = vec![0u8; header.datasize() as usize];
        if let Err(e) = self.reader.read_exact(&mut buf) {
            self.last_blob_ok = false;
            return Some(Err(crate::error::new_error(crate::error::ErrorKind::Io(e))));
        }

        self.offset = self
            .offset
            .map(|x| ByteOffset(x.0 + header.datasize() as u64));

        let range = prev_offset.and_then(|prev| {
            self.offset.map(|end| BlobRange {
                data_start: ByteOffset(prev.0 + header_size),
                data_end: end,
            })
        });

        self.blob_ranges.push(range.unwrap());

        Some(Ok(RawBlob {
            header,
            data: buf,
        }))
    }
}

/// Parallel stage: decode RawBlob -> Blob
#[derive(Debug)]
pub struct ParBlobIterator<I>
where
    I: Iterator<Item=crate::Result<RawBlob>> + Send,
{
    pub raw_iter: I,
}

impl<I> ParBlobIterator<I>
where
    I: Iterator<Item=crate::Result<RawBlob>> + Send,
{
    pub fn new(raw_iter: I) -> Self {
        Self { raw_iter }
    }

    pub fn par_bridge(mut self) -> impl ParallelIterator<Item=crate::Result<(usize, Blob)>> {
        self.raw_iter
            .enumerate()
            .par_bridge()
            .map(|(i, res)| match res {
                Ok(raw) => {
                    let blob = fileformat::Blob::parse_from_bytes(&raw.data)
                        .map_err(|e| new_protobuf_error(e, "blob content"))?;
                    Ok((i, Blob::new(raw.header, blob)))
                }
                Err(e) => Err(e),
            })
    }
}


pub struct BlobRangeReader<'a, R>
where
    R: Read + Seek,
{
    reader: R,
    blob_ranges: &'a Vec<BlobRange>,
    index: usize,
    end_index: usize,
}

impl<'a, R> BlobRangeReader<'a, R>
where
    R: Read + Seek,
{
    /// Create a new reader from a seekable source and blob range subset
    pub fn new(source: R, blob_ranges: &'a Vec<BlobRange>, start_idx: usize, end_idx: usize) -> Self {
        let end_idx = end_idx.min(blob_ranges.len());
        Self {
            reader: source,
            blob_ranges,
            index: start_idx,
            end_index: end_idx,
        }
    }
}

impl<'a, R: Read + Seek> Iterator for BlobRangeReader<'a, R> {
    type Item = crate::Result<RawBlob>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.end_index {
            return None;
        }
        let blob_range = self.blob_ranges.get(self.index)?;
        let size = blob_range.data_end.0 - blob_range.data_start.0;
        let mut buf = vec![0u8; size as usize];
        self.reader.seek(SeekFrom::Start(blob_range.data_start.0));
        self.reader.read_exact(&mut buf);
        self.index += 1;
        Some(Ok(RawBlob {
            header: fileformat::BlobHeader::default(),
            data: buf,
        }))
    }
}
