/*!
A fast reader for the OpenStreetMap PBF file format (\*.osm.pbf).

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
osmpbf = "0.2"
```

and if you're using Rust 2015, add this line to the crate root:

```rust
extern crate osmpbf;
```

## Example: Count ways

Here's a simple example that counts all the OpenStreetMap way elements in a
file:

```rust
use osmpbf::{ElementReader, Element};

let reader = ElementReader::from_path("tests/test.osm.pbf").unwrap();
let mut ways = 0_u64;

// Increment the counter by one for each way.
reader.for_each(|element| {
    if let Element::Way(_) = element {
        ways += 1;
    }
}).unwrap();

println!("Number of ways: {}", ways);
```

## Example: Count ways in parallel

In this second example, we also count the ways but make use of all cores by
decoding the file in parallel:

```rust
use osmpbf::{ElementReader, Element};

let reader = ElementReader::from_path("tests/test.osm.pbf").unwrap();

// Count the ways
let ways = reader.par_map_reduce(
    |element| {
        match element {
            Element::Way(_) => 1,
            _ => 0,
        }
    },
    || 0_u64,      // Zero is the identity value for addition
    |a, b| a + b   // Sum the partial results
).unwrap();

println!("Number of ways: {}", ways);
```
*/

#![recursion_limit = "1024"]

extern crate byteorder;
extern crate memmap;
extern crate protobuf;
extern crate rayon;

#[cfg(feature = "system-libz")]
extern crate flate2;

#[cfg(not(feature = "system-libz"))]
extern crate inflate;

pub use blob::*;
pub use block::*;
pub use dense::*;
pub use elements::*;
pub use error::{BlobError, Error, ErrorKind, Result};
pub use indexed::*;
pub use mmap_blob::*;
pub use reader::*;

pub mod blob;
pub mod block;
pub mod dense;
pub mod elements;
mod error;
pub mod indexed;
pub mod mmap_blob;
mod proto;
pub mod reader;
mod util;
