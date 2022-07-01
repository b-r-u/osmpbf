// Count the number of buildings and their nodes from a PBF file
// given as the first command line argument.

use osmpbf::{Element, IndexedReader};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Read command line argument and create IndexedReader
    let arg = std::env::args_os()
        .nth(1)
        .ok_or("need a *.osm.pbf file as argument")?;
    let mut reader = IndexedReader::from_path(&arg)?;

    println!("Counting...");
    let mut ways = 0;
    let mut nodes = 0;

    reader.read_ways_and_deps(
        |way| {
            // Filter ways. Return true if tags contain "building": "yes".
            way.tags().any(|key_value| key_value == ("building", "yes"))
        },
        |element| {
            // Increment counter for ways and nodes
            match element {
                Element::Way(_way) => ways += 1,
                Element::Node(_node) => nodes += 1,
                Element::DenseNode(_dense_node) => nodes += 1,
                Element::Relation(_) => {} // should not occur
            }
        },
    )?;

    // Print result
    println!("ways:  {ways}\nnodes: {nodes}");
    Ok(())
}
