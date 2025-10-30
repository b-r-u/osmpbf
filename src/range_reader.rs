// use crate::blob::{Blob, BlobReader, ByteOffset};
// use std::fs::File;
// use std::io::{BufReader, Read, Seek, SeekFrom};
// use std::path::Path;
// use protobuf::Message;
// use crate::error::Result;
// use crate::InformedReaderIter;
// use crate::BlobRange;
//
// #[derive(Debug)]
// pub struct RangeReaderIter {
//     blob_iter: BlobReader<BufReader<File>>,
//     index: usize,
//     end_index: usize,
//     blobs: Vec<BlobRange>,
// }
//
// const BUFFER_SIZE: usize = 1024 * 1024;
// impl RangeReaderIter {
//     pub fn from_path<P: AsRef<Path>>(path: P, start: usize, end:usize, blobs: Vec<BlobRange>) -> Result<Self> {
//         let file = File::open(path)?;
//         let buf_reader = BufReader::with_capacity(BUFFER_SIZE, file);
//         let blob_iter = BlobReader::new(buf_reader);
//         Ok(Self {
//             blob_iter,
//             index: start,
//             end_index: end,
//             blobs,
//         })
//     }
// }
//
// impl Iterator for RangeReaderIter {
//
//     type Item = Result<Blob>;
//     fn next(&mut self) -> Option<Self::Item> {
//
//         if self.index >= self.end_index {
//             return None
//         }
//
//         let range = self.blobs[self.index];
//
//         if let Err(e) = self.blob_iter.reader.seek(SeekFrom::Start(range.data_start as u64)) {
//             return Some(Err(crate::error::new_error(
//                 crate::error::ErrorKind::Io(e),
//             )));
//         }
//
//         let size = (range.data_end.0 - range.data_start.0) as usize;
//         let mut buf = vec![0u8; size];
//         if let Err(e) = self.blob_iter.reader.read_exact(&mut buf) {
//             return Some(Err(crate::error::new_error(
//                 crate::error::ErrorKind::Io(e),
//             )));
//         }
//
//         let blob = match crate::proto::fileformat::Blob::parse_from_bytes(&buf) {
//             Ok(b) => Blob::from_data(b, Some(range.data_start)),
//             Err(e) => {
//                 return Some(Err(crate::error::new_protobuf_error(
//                     e,
//                     "blob data",
//                 )))
//             }
//         };
//
//
//         let blob_res = self.blob_iter.next()?;
//
//         // Record offset if available
//         let offset = blob_res
//             .as_ref()
//             .ok()
//             .and_then(|b| b.offset())
//             .unwrap_or(ByteOffset(0));
//
//         self.blob_offsets.push(offset);
//
//         let idx = self.index;
//         self.index += 1;
//         Some((idx, blob_res))
//     }
// }