// Count the number of nodes, ways and relations in a PBF file given as the
// first command line argument.

use osmpbf::*;

fn main() {
    let arg = std::env::args_os()
        .nth(1)
        .expect("need a *.osm.pbf file as argument");
    let path = std::path::Path::new(&arg);
    let reader = ElementReader::from_path(path).unwrap();

    println!("Counting...");

    match reader.par_map_reduce(
        |element| match element {
            Element::Node(_) | Element::DenseNode(_) => (1, 0, 0),
            Element::Way(_) => (0, 1, 0),
            Element::Relation(_) => (0, 0, 1),
        },
        || (0u64, 0u64, 0u64),
        |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2),
    ) {
        Ok((nodes, ways, relations)) => {
            println!("Nodes: {nodes}");
            println!("Ways: {ways}");
            println!("Relations: {relations}");
        }
        Err(e) => {
            println!("{e}");
            std::process::exit(1);
        }
    }
}
