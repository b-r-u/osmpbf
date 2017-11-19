//! Read and decode blobs

extern crate protobuf;
extern crate byteorder;

use block::{HeaderBlock, PrimitiveBlock};
use byteorder::ReadBytesExt;
use errors::*;
use proto::fileformat;
use std::fs::File;
use std::io::{BufReader, ErrorKind, Read};
use std::path::Path;

#[cfg(feature = "system-libz")]
use flate2::read::ZlibDecoder;

#[cfg(not(feature = "system-libz"))]
use inflate::DeflateDecoder;


/// The content type of a blob.
#[derive(Debug, Eq, PartialEq)]
pub enum BlobType<'a> {
    /// Blob contains a `HeaderBlock`.
    OsmHeader,
    /// Blob contains a `PrimitiveBlock`.
    OsmData,
    /// An unknown blob type with the given string identifier.
    /// Parsers should ignore unknown blobs they do not expect.
    Unknown(&'a str),
}

//TODO rename variants to fit proto files
/// The decoded content of a blob (analogous to `BlobType`).
pub enum BlobDecode<'a> {
    /// Blob contains a `HeaderBlock`.
    OsmHeader(HeaderBlock),
    /// Blob contains a `PrimitiveBlock`.
    OsmData(PrimitiveBlock),
    /// An unknown blob type with the given string identifier.
    /// Parsers should ignore unknown blobs they do not expect.
    Unknown(&'a str),
}

/// A blob.
///
/// A PBF file consists of a sequence of blobs. This type supports decoding the content of a blob
/// to different types of blocks that are usually more interesting to the user.
pub struct Blob {
    header: fileformat::BlobHeader,
    blob: fileformat::Blob,
}

impl Blob {
    fn new(header: fileformat::BlobHeader, blob: fileformat::Blob) -> Blob {
        Blob {
            header: header,
            blob: blob
        }
    }

    /// Decodes the Blob and tries to obtain the inner content (usually a `HeaderBlock` or a
    /// `PrimitiveBlock`). This operation might involve an expensive decompression step.
    pub fn decode(&self) -> Result<BlobDecode> {
        match self.get_type() {
            BlobType::OsmHeader => {
                self.to_headerblock()
                    .map(BlobDecode::OsmHeader)
            },
            BlobType::OsmData => {
                self.to_primitiveblock()
                    .map(BlobDecode::OsmData)
            },
            BlobType::Unknown(x) => Ok(BlobDecode::Unknown(x)),
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

    /// Tries to decode the blob to a `HeaderBlock`. This operation might involve an expensive
    /// decompression step.
    pub fn to_headerblock(&self) -> Result<HeaderBlock> {
        decode_blob(&self.blob)
            .map(HeaderBlock::new)
            .chain_err(|| "failed to decode blob to header block")
    }

    /// Tries to decode the blob to a `PrimitiveBlock`. This operation might involve an expensive
    /// decompression step.
    pub fn to_primitiveblock(&self) -> Result<PrimitiveBlock> {
        decode_blob(&self.blob)
            .map(PrimitiveBlock::new)
            .chain_err(|| "failed to decode blob to primitive block")
    }
}

/// A reader for PBF files that allows iterating over `Blob`s.
pub struct BlobReader<R: Read> {
    reader: R,
    last_blob_ok: bool,
}

impl<R: Read> BlobReader<R> {
    /// Creates a new `ElementReader`.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let f = std::fs::File::open("tests/test.osm.pbf")?;
    /// let buf_reader = std::io::BufReader::new(f);
    ///
    /// let reader = ElementReader::new(buf_reader);
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(reader: R) -> BlobReader<R> {
        BlobReader {
            reader: reader,
            last_blob_ok: true,
        }
    }
}

impl BlobReader<BufReader<File>> {
    /// Tries to open the file at the given path and constructs a `BlobReader` from this.
    ///
    /// # Errors
    /// Returns the same errors that `std::fs::File::open` returns.
    ///
    /// # Example
    /// ```
    /// use osmpbf::*;
    ///
    /// # fn foo() -> Result<()> {
    /// let reader = BlobReader::from_path("tests/test.osm.pbf")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self>
    {
        let f = File::open(path)?;
        let reader = BufReader::new(f);

        Ok(BlobReader::new(reader))
    }
}

impl<R: Read> Iterator for BlobReader<R> {
    type Item = Result<Blob>;

    fn next(&mut self) -> Option<Self::Item> {
        // Stop iteration if there was an error.
        if !self.last_blob_ok {
            return None;
        }

        let size: u64 = match self.reader.read_u32::<byteorder::BigEndian>() {
            Ok(n) => u64::from(n),
            Err(e) => {
                match e.kind() {
                    ErrorKind::UnexpectedEof => {
                        return None
                    },
                    _ => {
                        self.last_blob_ok = false;
                        return Some(Err(Error::with_chain(e, "Could not decode blob size")));
                    },
                }
            },
        };

        let header: fileformat::BlobHeader = match protobuf::parse_from_reader(&mut self.reader.by_ref().take(size)) {
            Ok(header) => header,
            Err(e) => {
                self.last_blob_ok = false;
                return Some(Err(Error::with_chain(e, "Could not decode BlobHeader")));
            },
        };

        let blob: fileformat::Blob = match protobuf::parse_from_reader(&mut self.reader.by_ref().take(header.get_datasize() as u64)) {
            Ok(blob) => blob,
            Err(e) => {
                self.last_blob_ok = false;
                return Some(Err(Error::with_chain(e, "Could not decode Blob")));
            },
        };

        Some(Ok(Blob::new(header, blob)))
    }
}

#[cfg(feature = "system-libz")]
pub(crate) fn decode_blob<T>(blob: &fileformat::Blob) -> Result<T>
    where T: protobuf::Message + protobuf::MessageStatic {
    if blob.has_raw() {
        protobuf::parse_from_bytes(blob.get_raw()).chain_err(|| "Could not parse raw data")
    } else if blob.has_zlib_data() {
        let mut decoder = ZlibDecoder::new(blob.get_zlib_data());
        protobuf::parse_from_reader(&mut decoder).chain_err(|| "Could not parse zlib data")
    } else {
        bail!("Blob is missing fields 'raw' and 'zlib_data")
    }
}

#[cfg(not(feature = "system-libz"))]
pub(crate) fn decode_blob<T>(blob: &fileformat::Blob) -> Result<T>
    where T: protobuf::Message + protobuf::MessageStatic {
    if blob.has_raw() {
        protobuf::parse_from_bytes(blob.get_raw()).chain_err(|| "Could not parse raw data")
    } else if blob.has_zlib_data() {
        let mut decoder = DeflateDecoder::from_zlib(blob.get_zlib_data());
        protobuf::parse_from_reader(&mut decoder).chain_err(|| "Could not parse zlib data")
    } else {
        bail!("Blob is missing fields 'raw' and 'zlib_data")
    }
}
