use crate::error::{new_blob_error, new_protobuf_error};
use crate::proto::fileformat;
use crate::{Blob, BlobError, BlobRange, ByteOffset, MAX_BLOB_HEADER_SIZE};
use byteorder::ReadBytesExt;
use protobuf::Message;
use rayon::prelude::*;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

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

    fn read_blob_header(&mut self) -> Option<crate::Result<fileformat::BlobHeader>> {
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
        Some(Ok(header))
    }
}

impl<R: Read + Send> Iterator for RawBlobReader<'_, R> {
    type Item = crate::Result<RawBlob>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.last_blob_ok {
            return None;
        }

        let header = match self.read_blob_header()? {
            Ok(h) => h,
            Err(e) => return Some(Err(e)),
        };

        let start_offset = self.offset;

        let mut buf = vec![0u8; header.datasize() as usize];
        if let Err(e) = self.reader.read_exact(&mut buf) {
            self.last_blob_ok = false;
            return Some(Err(crate::error::new_error(crate::error::ErrorKind::Io(e))));
        }

        self.offset = self
            .offset
            .map(|x| ByteOffset(x.0 + header.datasize() as u64));

        let range = start_offset.and_then(|start| {
            self.offset.map(|end| BlobRange {
                data_start: start,
                data_end: end,
            })
        });

        self.blob_ranges.push(range.unwrap());

        Some(Ok(RawBlob { header, data: buf }))
    }
}

/// Parallel stage: decode RawBlob -> Blob
#[derive(Debug)]
pub struct ParBlobIterator<I>
where
    I: Iterator<Item = crate::Result<RawBlob>> + Send,
{
    pub raw_iter: I,
}

impl<I> ParBlobIterator<I>
where
    I: Iterator<Item = crate::Result<RawBlob>> + Send,
{
    pub fn new(raw_iter: I) -> Self {
        Self { raw_iter }
    }

    pub fn par_bridge(self, chunk_size: usize) -> impl ParallelIterator<Item = crate::Result<Blob>> {
        self.raw_iter.par_bridge().map(|res| match res {
            Ok(raw) => {
                let blob = fileformat::Blob::parse_from_bytes(&raw.data)
                    .map_err(|e| new_protobuf_error(e, "blob content"))?;
                Ok(Blob::new(raw.header, blob))
            }
            Err(e) => Err(e),
        })
    }
    pub fn par_bridge_enumerated(
        self,
    ) -> impl ParallelIterator<Item = crate::Result<(usize, Blob)>> {
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

    pub fn iter(self, chunk_size: usize) -> impl Iterator<Item = crate::Result<Blob>> {
        self.raw_iter.map(|res| match res {
            Ok(raw) => {
                let blob = fileformat::Blob::parse_from_bytes(&raw.data)
                    .map_err(|e| new_protobuf_error(e, "blob content"))?;
                Ok(Blob::new(raw.header, blob))
            }
            Err(e) => Err(e),
        })
    }

    pub fn iter_enumerated(self, chunk_size: usize) -> impl Iterator<Item = crate::Result<(usize, Blob)>> {
        self.raw_iter.enumerate().map(|(i, res)| match res {
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
    reader: BufReader<R>,
    blob_ranges: &'a Vec<BlobRange>,
    index: usize,
    end_index: usize,
}

impl<'a, R> BlobRangeReader<'a, R>
where
    R: Read + Seek,
{
    /// Create a new reader from a seekable source and blob range subset
    pub fn new(
        source: BufReader<R>,
        blob_ranges: &'a Vec<BlobRange>,
        start_idx: usize,
        end_idx: usize,
    ) -> Self {
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
        let blob_range = match self.blob_ranges.get(self.index) {
            Some(range) => range,
            None => return None,
        };

        let target_start = blob_range.data_start.0;
        let size = (blob_range.data_end.0 - target_start) as usize;
        let mut buf = vec![0u8; size];

        // --- Start Buffer Check and Move Logic ---

        // 1. Get the current position *of the underlying reader*.
        // `stream_position()` gives the position *after* the buffer content.
        let pos_after_buffer = match self.reader.stream_position() {
            Ok(p) => p,
            Err(e) => return Some(Err(crate::error::new_error(crate::error::ErrorKind::Io(e)))),
        };

        // 2. Calculate the position of the *start* of the current buffer.
        let buffer_len = self.reader.buffer().len() as u64;
        let pos_at_buffer_start = pos_after_buffer.checked_sub(buffer_len).unwrap_or(0);

        // 3. Determine if the target data starts *within* the current buffer.
        let is_in_buffer = target_start >= pos_at_buffer_start
            && target_start < pos_after_buffer;

        if is_in_buffer {
            // Calculate how many bytes to skip/consume from the start of the buffer
            let offset_to_consume = (target_start - pos_at_buffer_start) as usize;

            // Advance the buffer pointer by consuming the necessary bytes.
            self.reader.consume(offset_to_consume);
        } else {
            // The target is not in the current buffer (either before or far after).
            // A seek is mandatory, and it will invalidate the current buffer.
            if let Err(e) = self.reader.seek(SeekFrom::Start(target_start)) {
                self.index += 1;
                return Some(Err(crate::error::new_error(crate::error::ErrorKind::Io(e))));
            }
        }

        // --- End Buffer Check and Move Logic ---

        // 4. Read the exact number of bytes. If the read starts in the buffer,
        // `read_exact` will use the remaining buffer contents and then refill if needed.
        if let Err(e) = self.reader.read_exact(&mut buf) {
            self.index += 1;
            return Some(Err(crate::error::new_error(crate::error::ErrorKind::Io(e))));
        }

        self.index += 1;
        Some(Ok(RawBlob {
            header: fileformat::BlobHeader::default(),
            data: buf,
        }))
    }
}

// fn chunk_iter<T>(a: impl IntoIterator<Item = Option<T>>, chunk_size: usize) -> impl Iterator<Item = Vec<T>> {
//     let mut a = a.into_iter();
//     std::iter::from_fn(move || {
//         Some(a.by_ref().take(chunk_size).)
//     })
// }