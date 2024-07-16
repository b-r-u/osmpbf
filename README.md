osmpbf
======
A Rust library for reading the OpenStreetMap PBF file format (\*.osm.pbf). It
strives to offer the best performance using parallelization and lazy-decoding
with a simple interface while also exposing iterators for items of every level
in a PBF file.

[![Build status](https://github.com/b-r-u/osmpbf/actions/workflows/ci.yml/badge.svg)](https://github.com/b-r-u/osmpbf/actions)
[![Build status](https://ci.appveyor.com/api/projects/status/1ct6i2gjsak8tgyy?svg=true)](https://ci.appveyor.com/project/b-r-u/osmpbf)
[![Crates.io](https://img.shields.io/crates/v/osmpbf.svg)](https://crates.io/crates/osmpbf)
[![Documentation](https://docs.rs/osmpbf/badge.svg)](https://docs.rs/osmpbf)

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
osmpbf = "0.3"
```

Here's a simple example that counts all the ways in a file:

```rust
use osmpbf::{ElementReader, Element};

let reader = ElementReader::from_path("tests/test.osm.pbf")?;
let mut ways = 0_u64;

// Increment the counter by one for each way.
reader.for_each(|element| {
    if let Element::Way(_) = element {
        ways += 1;
    }
})?;

println!("Number of ways: {ways}");
```

In this second example, we also count the ways but make use of all cores by
decoding the file in parallel:

```rust
use osmpbf::{ElementReader, Element};

let reader = ElementReader::from_path("tests/test.osm.pbf")?;

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
)?;

println!("Number of ways: {ways}");
```

## Build Features
* `rust-zlib` (default) -- use the pure Rust zlib implementation`miniz_oxide`
* `zlib` -- use the widely available `zlib` library
* `zlib-ng` -- use the `zlib-ng` library for better performance.

## The PBF format

To effectively use the more lower-level features of this library it is useful to
have an overview of the structure of a PBF file. For a more detailed format
description see [here](http://wiki.openstreetmap.org/wiki/PBF_Format) or take a
look at the `.proto` files in this repository.

The PBF format as a hierarchy (square brackets `[]` denote arrays):
```
Blob[]
├── HeaderBlock
└── PrimitiveBlock
    └── PrimitiveGroup[]
    	├── Node[]
    	├── DenseNodes
    	├── Way[]
        └── Relation[]
```

At the highest level a PBF file consists of a sequence of blobs. Each `Blob` can
be decoded into either a `HeaderBlock` or a `PrimitiveBlock`.

Iterating over blobs is very fast, but decoding might involve a more expensive
decompression step. So especially for larger files it is advisable to
parallelize at the blob level as each blob can be decompressed independently.
(See the `reader` module in this library for parallel methods)

Usually the first `Blob` of a file decodes to a `HeaderBlock` which holds global
information for all following `PrimitiveBlocks`, such as a list of required
parser features.

A `PrimitiveBlock` contains an array of `PrimitiveGroup`s. Each `PrimitiveGroup`
only contains one element type: `Node`, `Way`, `Relation` or `DenseNodes`. A
`DenseNodes` item is an alternative and space-saving representation of a `Node`
array. So, do not forget to check for `DenseNodes` when aggregating all nodes in
a file.

Elements reference each other using integer IDs. Corresponding elements could be
stored in any blob, so finding them can involve iterating over the whole file.
Some files declare an optional feature "Sort.Type\_then\_ID" in the
`HeaderBlock` to indicate that elements are stored sorted by their type and then
ID. This can be used to dramatically reduce the search space.

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.
