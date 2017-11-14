extern crate osmpbf;

use osmpbf::*;

fn main() {
    let arg = std::env::args_os().nth(1).expect("need a *.osm.pbf file as argument");
    let path = std::path::Path::new(&arg);
    let reader = ElementReader::from_path(path).unwrap();

    println!("Counting...");

    let result = reader.par_map_reduce(
        |element| {
            match element {
                Element::Node(_) | Element::DenseNode(_) => (1, 0, 0),
                Element::Way(_) => (0, 1, 0),
                Element::Relation(_) => (0, 0, 1),
            }
        },
        || (0u64, 0u64, 0u64),
        |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2)
    ).unwrap();

    println!("Nodes: {}", result.0);
    println!("Ways: {}", result.1);
    println!("Relations: {}", result.2);
}
