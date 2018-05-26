use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::result;
use std::str;
use std::str::Utf8Error;

use protobuf::ProtobufError;

// Error data structures are modeled just like in the `csv` crate by BurntSushi.

pub(crate) fn new_error(kind: ErrorKind) -> Error {
    Error(Box::new(kind))
}

pub(crate) fn new_blob_error(kind: BlobError) -> Error {
    Error(Box::new(ErrorKind::Blob(kind)))
}

pub(crate) fn new_protobuf_error(err: ProtobufError, location: &'static str) -> Error {
    Error(Box::new(ErrorKind::Protobuf{ err, location }))
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
#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    Protobuf{err: ProtobufError, location: &'static str},
    Utf8(Utf8Error),
    StringtableIndexOutOfBounds{index: usize},
    Blob(BlobError),

    //TODO add UnexpectedPrimitiveBlock

    /// Hints that destructuring should not be exhaustive.
    ///
    /// This enum may grow additional variants, so this makes sure clients
    /// don't count on exhaustive matching. (Otherwise, adding a new variant
    /// could break existing code.)
    #[doc(hidden)]
    __Nonexhaustive,
}

/// An error that occurs when decoding a blob.
#[derive(Debug)]
pub enum BlobError {
    InvalidHeaderSize,
    HeaderTooBig{size: u64},
    MessageTooBig{size: u64},
    Empty,
    /// Hints that destructuring should not be exhaustive.
    #[doc(hidden)]
    __Nonexhaustive,
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
            ErrorKind::Io(ref err) => err.description(),
            ErrorKind::Protobuf{ref err, ..} => err.description(),
            ErrorKind::Utf8(ref err) => err.description(),
            ErrorKind::StringtableIndexOutOfBounds{..} => "stringtable index out of bounds",
            ErrorKind::Blob(BlobError::InvalidHeaderSize) => "blob header size could not be decoded",
            ErrorKind::Blob(BlobError::HeaderTooBig{..}) => "blob header is too big",
            ErrorKind::Blob(BlobError::MessageTooBig{..}) => "blob message is too big",
            ErrorKind::Blob(BlobError::Empty) => "blob is missing fields 'raw' and 'zlib_data",
            _ => unreachable!(),
        }
    }

    fn cause(&self) -> Option<&StdError> {
        match *self.0 {
            ErrorKind::Io(ref err) => Some(err),
            ErrorKind::Protobuf{ref err, ..} => Some(err),
            ErrorKind::Utf8(ref err) => Some(err),
            ErrorKind::StringtableIndexOutOfBounds{..} => None,
            ErrorKind::Blob(BlobError::InvalidHeaderSize) => None,
            ErrorKind::Blob(BlobError::HeaderTooBig{..}) => None,
            ErrorKind::Blob(BlobError::MessageTooBig{..}) => None,
            ErrorKind::Blob(BlobError::Empty) => None,
            _ => unreachable!(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            ErrorKind::Io(ref err) => err.fmt(f),
            ErrorKind::Protobuf{ref err, location} => {
                write!(f, "protobuf error at '{}': {}", location, err)
            },
            ErrorKind::Utf8(ref err) => err.fmt(f),
            ErrorKind::StringtableIndexOutOfBounds { index } => {
                write!(f, "stringtable index out of bounds: {}", index)
            },
            ErrorKind::Blob(BlobError::InvalidHeaderSize) => {
                write!(f, "blob header size could not be decoded")
            },
            ErrorKind::Blob(BlobError::HeaderTooBig { size }) => {
                write!(f, "blob header is too big: {} bytes", size)
            },
            ErrorKind::Blob(BlobError::MessageTooBig { size }) => {
                write!(f, "blob message is too big: {} bytes", size)
            },
            ErrorKind::Blob(BlobError::Empty) => {
                write!(f, "blob is missing fields 'raw' and 'zlib_data'")
            },
            _ => unreachable!(),
        }
    }
}

