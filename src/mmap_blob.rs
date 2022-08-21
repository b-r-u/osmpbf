//! Iterate over blobs from a memory map

use self::fileformat::BlobHeader;
use crate::blob::{decode_blob, BlobDecode, BlobType, ByteOffset};
use crate::block::{HeaderBlock, PrimitiveBlock};
use crate::error::{new_blob_error, new_protobuf_error, BlobError, Result};
use crate::proto::{fileformat, osmformat};
use crate::MAX_BLOB_HEADER_SIZE;
use byteorder::ByteOrder;
use protobuf::Message;
use std::fs::File;
use std::path::Path;

/// A read-only memory map.
#[derive(Debug)]
pub struct Mmap {
    mmap: memmap2::Mmap,
}

impl Mmap {
    /// Creates a memory map from a given file.
    ///
    /// # Safety
    /// The underlying file should not be modified while holding the memory map.
    /// See [memmap-rs issue 25](https://github.com/danburkert/memmap-rs/issues/25) for more
    /// information on the safety of memory maps.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let f = std::fs::File::open("tests/test.osm.pbf")?;
    /// let mmap = unsafe { Mmap::from_file(&f)? };
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub unsafe fn from_file(file: &File) -> Result<Mmap> {
        memmap2::Mmap::map(file)
            .map(|m| Mmap { mmap: m })
            .map_err(|e| e.into())
    }

    /// Creates a memory map from a given path.
    ///
    /// # Safety
    /// The underlying file should not be modified while holding the memory map.
    /// See [memmap-rs issue 25](https://github.com/danburkert/memmap-rs/issues/25) for more
    /// information on the safety of memory maps.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let mmap = unsafe { Mmap::from_path("tests/test.osm.pbf")? };
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> Result<Mmap> {
        let file = File::open(&path)?;
        memmap2::Mmap::map(&file)
            .map(|m| Mmap { mmap: m })
            .map_err(|e| e.into())
    }

    /// Returns an iterator over the blobs in this memory map.
    pub fn blob_iter(&self) -> MmapBlobReader {
        MmapBlobReader::new(self)
    }

    fn as_slice(&self) -> &[u8] {
        &self.mmap
    }
}

/// A PBF blob from a memory map.
#[derive(Clone, Debug)]
pub struct MmapBlob<'a> {
    header: BlobHeader,
    data: &'a [u8],
    offset: ByteOffset,
}

impl<'a> MmapBlob<'a> {
    /// Decodes the blob and tries to obtain the inner content (usually a [`HeaderBlock`] or a
    /// [`PrimitiveBlock`]). This operation might involve an expensive decompression step.
    pub fn decode(&'a self) -> Result<BlobDecode<'a>> {
        let blob = fileformat::Blob::parse_from_bytes(self.data)
            .map_err(|e| new_protobuf_error(e, "blob content"))?;
        match self.header.type_() {
            "OSMHeader" => {
                let block = Box::new(HeaderBlock::new(decode_blob(&blob)?));
                Ok(BlobDecode::OsmHeader(block))
            }
            "OSMData" => {
                let block: osmformat::PrimitiveBlock = decode_blob(&blob)?;
                Ok(BlobDecode::OsmData(PrimitiveBlock::new(block)))
            }
            x => Ok(BlobDecode::Unknown(x)),
        }
    }

    /// Returns the type of a blob without decoding its content.
    pub fn get_type(&self) -> BlobType {
        match self.header.type_() {
            "OSMHeader" => BlobType::OsmHeader,
            "OSMData" => BlobType::OsmData,
            x => BlobType::Unknown(x),
        }
    }

    /// Returns the byte offset of the blob from the start of its memory map.
    pub fn offset(&self) -> ByteOffset {
        self.offset
    }
}

/// A reader for memory mapped PBF files that allows iterating over [`MmapBlob`]s.
#[derive(Clone, Debug)]
pub struct MmapBlobReader<'a> {
    mmap: &'a Mmap,
    offset: usize,
    last_blob_ok: bool,
}

impl<'a> MmapBlobReader<'a> {
    /// Creates a new `MmapBlobReader`.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    ///
    /// let mmap = unsafe { Mmap::from_path("tests/test.osm.pbf")? };
    /// let reader = MmapBlobReader::new(&mmap);
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn new(mmap: &Mmap) -> MmapBlobReader {
        MmapBlobReader {
            mmap,
            offset: 0,
            last_blob_ok: true,
        }
    }

    /// Move the cursor to the given byte offset.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    ///
    /// let mmap = unsafe { Mmap::from_path("tests/test.osm.pbf")? };
    /// let mut reader = MmapBlobReader::new(&mmap);
    ///
    /// let first_blob = reader.next().unwrap()?;
    /// let second_blob = reader.next().unwrap()?;
    ///
    /// reader.seek(first_blob.offset());
    /// let first_blob_again = reader.next().unwrap()?;
    ///
    /// assert_eq!(first_blob.offset(), first_blob_again.offset());
    ///
    /// # Ok(())
    /// # }
    /// # foo().unwrap();
    /// ```
    pub fn seek(&mut self, pos: ByteOffset) {
        self.offset = pos.0 as usize;
    }
}

impl<'a> Iterator for MmapBlobReader<'a> {
    type Item = Result<MmapBlob<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = &self.mmap.as_slice()[self.offset..];

        match slice.len() {
            0 => return None,
            1..=3 => {
                self.last_blob_ok = false;
                return Some(Err(new_blob_error(BlobError::InvalidHeaderSize)));
            }
            _ => {}
        }

        let header_size = byteorder::BigEndian::read_u32(slice) as usize;

        if header_size as u64 >= MAX_BLOB_HEADER_SIZE {
            self.last_blob_ok = false;
            return Some(Err(new_blob_error(BlobError::HeaderTooBig {
                size: header_size as u64,
            })));
        }

        if slice.len() < 4 + header_size {
            self.last_blob_ok = false;
            let io_error = ::std::io::Error::new(
                ::std::io::ErrorKind::UnexpectedEof,
                "content too short for header",
            );
            return Some(Err(io_error.into()));
        }

        let header = match BlobHeader::parse_from_bytes(&slice[4..(4 + header_size)]) {
            Ok(x) => x,
            Err(e) => {
                self.last_blob_ok = false;
                return Some(Err(new_protobuf_error(e, "blob header")));
            }
        };

        let data_size = header.datasize() as usize;
        let chunk_size = 4 + header_size + data_size;

        if slice.len() < chunk_size {
            self.last_blob_ok = false;
            let io_error = ::std::io::Error::new(
                ::std::io::ErrorKind::UnexpectedEof,
                "content too short for block data",
            );
            return Some(Err(io_error.into()));
        }

        let prev_offset = self.offset;
        self.offset += chunk_size;

        Some(Ok(MmapBlob {
            header,
            data: &slice[(4 + header_size)..chunk_size],
            offset: ByteOffset(prev_offset as u64),
        }))
    }
}
