/*!
A fast reader for the OpenStreetMap PBF file format (\*.osm.pbf).

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
osmpbf = "0.1"
```

and this to your crate root:

```rust
extern crate osmpbf;
```

## Example: Count ways

Here's a simple example that counts all the OpenStreetMap way elements in a
file:

```rust
extern crate osmpbf;

use osmpbf::*;

fn main() {
    let reader = ElementReader::from_path("tests/test.osm.pbf").unwrap();
    let mut ways = 0_u64;

    // Increment the counter by one for each way.
    reader.for_each(|element| {
        if let Element::Way(_) = element {
            ways += 1;
        }
    }).unwrap();

    println!("Number of ways: {}", ways);
}
```

## Example: Count ways in parallel

In this second example, we also count the ways but make use of all cores by
decoding the file in parallel:

```rust
use osmpbf::*;

fn main() {
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
}
```
*/

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
