// use crate::blob::{Blob, BlobReader, ByteOffset};
// use std::fs::File;
// use std::io::BufReader;
// use std::path::Path;
// use crate::BlobRange;
// use crate::error::Result;
// 
// /// Iterator wrapper around `BlobReader` that provides blob index and offset.
// #[derive(Debug)]
// pub struct InformedReaderIter {
//     blob_iter: BlobReader<BufReader<File>>,
//     index: usize,
//     pub blob_ranges: Vec<BlobRange>,
// }
// 
// const BUFFER_SIZE: usize = 1024 * 1024;
// impl InformedReaderIter {
//     /// Create a new `InformedReaderIter` from a file path.
//     pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
//         let file = File::open(path)?;
//         let buf_reader = BufReader::with_capacity(BUFFER_SIZE, file);
//         let blob_iter = BlobReader::at_offset(buf_reader, Some(ByteOffset(0)));
//         Ok(Self {
//             blob_iter,
//             index: 0,
//             blob_ranges: Vec::new(),
//         })
//     }
// 
//     pub fn into_blob_ranges(mut self) -> Vec<BlobRange> {
//         self.blob_ranges
//     }
// }
// 
// impl Iterator for InformedReaderIter {
//     type Item = (usize, Result<Blob>);
// 
//     fn next(&mut self) -> Option<Self::Item> {
//         let blob_res = self.blob_iter.next()?;
// 
//         // Record offset if available
//         let blob_range = blob_res
//             .as_ref()
//             .ok()
//             .and_then(|blob| blob.range.clone());
// 
//         self.blob_ranges.push(blob_range.expect("blob range does note exist"));
// 
//         let idx = self.index;
//         self.index += 1;
//         Some((idx, blob_res))
//     }
// }
