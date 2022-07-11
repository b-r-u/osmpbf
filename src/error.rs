use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::result;
use std::str;
use std::str::Utf8Error;

use protobuf::Error as ProtobufError;

// Error data structures are modeled just like in the `csv` crate by BurntSushi.

pub(crate) fn new_error(kind: ErrorKind) -> Error {
    Error(Box::new(kind))
}

pub(crate) fn new_blob_error(kind: BlobError) -> Error {
    Error(Box::new(ErrorKind::Blob(kind)))
}

pub(crate) fn new_protobuf_error(err: ProtobufError, location: &'static str) -> Error {
    Error(Box::new(ErrorKind::Protobuf { err, location }))
}

/// A type alias for `Result<T, osmpbf::Error>`.
pub type Result<T> = result::Result<T, Error>;

/// An error that can occur when reading PBF files.
#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl Error {
    /// Return the specific type of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    /// Unwrap this error into its underlying type.
    pub fn into_kind(self) -> ErrorKind {
        *self.0
    }
}

/// The specific type of an error.
#[non_exhaustive]
#[derive(Debug)]
pub enum ErrorKind {
    /// An error for I/O operations.
    Io(io::Error),
    /// An error that occurs when decoding a protobuf message.
    Protobuf {
        err: ProtobufError,
        location: &'static str,
    },
    /// The stringtable contains an entry at `index` that could not be decoded to a valid UTF-8
    /// string.
    StringtableUtf8 { err: Utf8Error, index: usize },
    /// An element contains an out-of-bounds index to the stringtable.
    StringtableIndexOutOfBounds { index: usize },
    /// An error that occurs when decoding `Blob`s.
    Blob(BlobError),
    //TODO add UnexpectedPrimitiveBlock
}

/// An error that occurs when decoding a blob.
#[non_exhaustive]
#[derive(Debug)]
pub enum BlobError {
    /// Header size could not be decoded to a u32.
    InvalidHeaderSize,
    /// Blob header is bigger than [`MAX_BLOB_HEADER_SIZE`](blob/MAX_BLOB_HEADER_SIZE.v.html).
    HeaderTooBig {
        /// Blob header size in bytes.
        size: u64,
    },
    /// Blob content is bigger than [`MAX_BLOB_MESSAGE_SIZE`](blob/MAX_BLOB_MESSAGE_SIZE.v.html).
    MessageTooBig {
        /// Blob content size in bytes.
        size: u64,
    },
    /// The blob is empty because the `raw` and `zlib-data` fields are missing.
    Empty,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        new_error(ErrorKind::Io(err))
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self.0 {
            ErrorKind::Io(ref err, ..) => {
                use std::io::ErrorKind;
                match err.kind() {
                    ErrorKind::NotFound => "io error: not found",
                    ErrorKind::PermissionDenied => "io error: permission denied",
                    ErrorKind::ConnectionRefused => "io error: connection refused",
                    ErrorKind::ConnectionReset => "io error: connection reset",
                    ErrorKind::ConnectionAborted => "io error: connection aborted",
                    ErrorKind::NotConnected => "io error: not connected",
                    ErrorKind::AddrInUse => "io error: address in use",
                    ErrorKind::AddrNotAvailable => "io error: address not available",
                    ErrorKind::BrokenPipe => "io error: broken pipe",
                    ErrorKind::AlreadyExists => "io error: already exists",
                    ErrorKind::WouldBlock => "io error: would block",
                    ErrorKind::InvalidInput => "io error: invalid input",
                    ErrorKind::InvalidData => "io error: invalid data",
                    ErrorKind::TimedOut => "io error: timed out",
                    ErrorKind::WriteZero => "io error: write zero",
                    ErrorKind::Interrupted => "io error: interrupted",
                    ErrorKind::Other => "io error: other",
                    ErrorKind::UnexpectedEof => "io error: unexpected EOF",
                    _ => "io error",
                }
            }
            ErrorKind::Protobuf { .. } => "protobuf error",
            ErrorKind::StringtableUtf8 { .. } => "UTF-8 error in stringtable",
            ErrorKind::StringtableIndexOutOfBounds { .. } => "stringtable index out of bounds",
            ErrorKind::Blob(BlobError::InvalidHeaderSize) => {
                "blob header size could not be decoded"
            }
            ErrorKind::Blob(BlobError::HeaderTooBig { .. }) => "blob header is too big",
            ErrorKind::Blob(BlobError::MessageTooBig { .. }) => "blob message is too big",
            ErrorKind::Blob(BlobError::Empty) => "blob is missing fields 'raw' and 'zlib_data",
        }
    }

    fn cause(&self) -> Option<&dyn StdError> {
        match *self.0 {
            ErrorKind::Io(ref err) => Some(err),
            ErrorKind::Protobuf { ref err, .. } => Some(err),
            ErrorKind::StringtableUtf8 { ref err, .. } => Some(err),
            ErrorKind::StringtableIndexOutOfBounds { .. } => None,
            ErrorKind::Blob(BlobError::InvalidHeaderSize) => None,
            ErrorKind::Blob(BlobError::HeaderTooBig { .. }) => None,
            ErrorKind::Blob(BlobError::MessageTooBig { .. }) => None,
            ErrorKind::Blob(BlobError::Empty) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            ErrorKind::Io(ref err) => err.fmt(f),
            ErrorKind::Protobuf { ref err, location } => {
                write!(f, "protobuf error at '{location}': {err}")
            }
            ErrorKind::StringtableUtf8 { ref err, index } => {
                write!(f, "invalid UTF-8 at string table index {index}: {err}")
            }
            ErrorKind::StringtableIndexOutOfBounds { index } => {
                write!(f, "stringtable index out of bounds: {index}")
            }
            ErrorKind::Blob(BlobError::InvalidHeaderSize) => {
                write!(f, "blob header size could not be decoded")
            }
            ErrorKind::Blob(BlobError::HeaderTooBig { size }) => {
                write!(f, "blob header is too big: {size} bytes")
            }
            ErrorKind::Blob(BlobError::MessageTooBig { size }) => {
                write!(f, "blob message is too big: {size} bytes")
            }
            ErrorKind::Blob(BlobError::Empty) => {
                write!(f, "blob is missing fields 'raw' and 'zlib_data'")
            }
        }
    }
}
