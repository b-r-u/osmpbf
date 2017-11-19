//! Iterate over blobs from a memory map

extern crate protobuf;
extern crate byteorder;
extern crate memmap;

use blob::{BlobDecode, BlobType, decode_blob};
use byteorder::ByteOrder;
use errors::*;
use block::{HeaderBlock, PrimitiveBlock};
use proto::{fileformat, osmformat};
use self::fileformat::BlobHeader;
use std::fs::File;
use std::path::Path;


/// A read-only memory map.
pub struct Mmap {
    mmap: memmap::Mmap,
}

impl Mmap {
    /// Creates a memory map from a given file.
    ///
    /// # Safety
    /// The underlying file should not be modified while holding the memory map.
    /// See https://github.com/danburkert/memmap-rs/issues/25
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
    /// ```
    pub unsafe fn from_file(file: &File) -> Result<Mmap> {
        memmap::Mmap::map(file)
            .map(|m| Mmap { mmap: m })
            .chain_err(|| "Could not create memory map from file")
    }

    /// Creates a memory map from a given path.
    ///
    /// # Safety
    /// The underlying file should not be modified while holding the memory map.
    /// See https://github.com/danburkert/memmap-rs/issues/25
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let mmap = unsafe { Mmap::from_path("tests/test.osm.pbf")? };
    /// # Ok(())
    /// # }
    /// ```
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> Result<Mmap> {
        let file = File::open(&path)?;
        memmap::Mmap::map(&file)
            .map(|m| Mmap { mmap: m })
            .chain_err(|| format!("Could not create memory map from path {}", path.as_ref().display()))
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
pub struct MmapBlob<'a> {
    header: BlobHeader,
    data: &'a [u8],
}

impl<'a> MmapBlob<'a> {
    /// Decodes the Blob and tries to obtain the inner content (usually a `HeaderBlock` or a
    /// `PrimitiveBlock`). This operation might involve an expensive decompression step.
    pub fn decode(&'a self) -> Result<BlobDecode<'a>> {
        let blob: fileformat::Blob = protobuf::parse_from_bytes(self.data)
            .chain_err(|| "failed to parse Blob")?;
        match self.header.get_field_type() {
            "OSMHeader" => {
                let block: osmformat::HeaderBlock = decode_blob(&blob).unwrap();
                Ok(BlobDecode::OsmHeader(HeaderBlock::new(block)))
            }
            "OSMData" => {
                let block: osmformat::PrimitiveBlock = decode_blob(&blob).unwrap();
                Ok(BlobDecode::OsmData(PrimitiveBlock::new(block)))
            }
            x => Ok(BlobDecode::Unknown(x)),
        }
    }

    /// Returns the type of a blob without decoding its content.
    pub fn get_type(&self) -> BlobType {
        match self.header.get_field_type() {
            "OSMHeader" => BlobType::OsmHeader,
            "OSMData" => BlobType::OsmData,
            x => BlobType::Unknown(x),
        }
    }
}

/// A reader for memory mapped PBF files that allows iterating over `MmapBlob`s.
#[derive(Clone)]
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
    /// ```
    pub fn new(mmap: &Mmap) -> MmapBlobReader {
        MmapBlobReader {
            mmap: mmap,
            offset: 0,
            last_blob_ok: true,
        }
    }
}

impl<'a> Iterator for MmapBlobReader<'a> {
    type Item = Result<MmapBlob<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = &self.mmap.as_slice()[self.offset..];

        match slice.len() {
            0 => return None,
            1 ... 3 => {
                self.last_blob_ok = false;
                let io_error = ::std::io::Error::new(
                    ::std::io::ErrorKind::UnexpectedEof, "failed to parse blob length"
                );
                return Some(Err(Error::from_kind(ErrorKind::Io(io_error))));
            },
            _ => {},
        }

        let header_size = byteorder::BigEndian::read_u32(slice) as usize;

        if slice.len() < 4 + header_size {
            self.last_blob_ok = false;
            let io_error = ::std::io::Error::new(
                ::std::io::ErrorKind::UnexpectedEof, "content too short for header"
            );
            return Some(Err(Error::from_kind(ErrorKind::Io(io_error))));
        }

        let header: BlobHeader = match protobuf::parse_from_bytes(&slice[4..(4 + header_size)]) {
            Ok(x) => x,
            Err(e) => {
                self.last_blob_ok = false;
                return Some(Err(e.into()));
            },
        };

        let data_size = header.get_datasize() as usize;
        let chunk_size = 4 + header_size + data_size;

        if slice.len() < chunk_size {
            self.last_blob_ok = false;
            let io_error = ::std::io::Error::new(
                ::std::io::ErrorKind::UnexpectedEof, "content too short for block data"
            );
            return Some(Err(Error::from_kind(ErrorKind::Io(io_error))));
        }

        self.offset += chunk_size;

        Some(Ok(MmapBlob {
            header: header,
            data: &slice[(4 + header_size)..chunk_size]
        }))
    }
}
