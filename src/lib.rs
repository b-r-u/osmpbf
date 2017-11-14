//! A fast reader for the PBF file format (\*.osm.pbf) for OpenStreetMap data.

#![recursion_limit = "1024"]

extern crate byteorder;
extern crate memmap;
extern crate protobuf;
extern crate rayon;
#[macro_use]
extern crate error_chain;

#[cfg(feature = "system-libz")]
extern crate flate2;

#[cfg(not(feature = "system-libz"))]
extern crate inflate;


pub use blob::*;
pub use block::*;
pub use dense::*;
pub use elements::*;
pub use errors::{Error, ErrorKind, Result, ResultExt};
pub use mmap_blob::*;
pub use reader::*;

mod errors;
mod proto;
pub mod reader;
pub mod blob;
pub mod block;
pub mod dense;
pub mod elements;
pub mod mmap_blob;

