use assert_approx_eq::assert_approx_eq;
use osmpbf::*;

static REQ_SCHEMA_V6: &str = "OsmSchema-V0.6";
static REQ_DENSE_NODES: &str = "DenseNodes";
static REQ_HIST_INFO: &str = "HistoricalInformation";
static OPT_LOC_ON_WAYS: &str = "LocationsOnWays";

struct TestFile {
    path: &'static str,
    req: &'static [&'static str],
    opt: &'static [&'static str],
}

static TEST_FILE_PATHS: &[TestFile; 3] = &[
    TestFile {
        path: "tests/test.osm.pbf",
        req: &[REQ_SCHEMA_V6, REQ_DENSE_NODES],
        opt: &[],
    },
    TestFile {
        path: "tests/test_nozlib.osm.pbf",
        req: &[REQ_SCHEMA_V6, REQ_DENSE_NODES],
        opt: &[],
    },
    TestFile {
        path: "tests/test_nozlib_nodense.osm.pbf",
        req: &[REQ_SCHEMA_V6],
        opt: &[],
    },
];

// This file was taken from the libosmium test suite.
// https://osmcode.org/libosmium/
static HISTORY_FILE_PATH: TestFile = TestFile {
    path: "tests/deleted_nodes.osh.pbf",
    req: &[REQ_SCHEMA_V6, REQ_DENSE_NODES, REQ_HIST_INFO],
    opt: &[],
};

// This file was generated with
// osmium add-locations-to-ways -i flex_mem tests/test.osm.pbf -o tests/loc_on_ways.osm.pbf
static LOC_ON_WAYS_FILE_PATH: TestFile = TestFile {
    path: "tests/loc_on_ways.osm.pbf",
    req: &[REQ_SCHEMA_V6, REQ_DENSE_NODES],
    opt: &[OPT_LOC_ON_WAYS],
};

// Helper functions to simplify testing
trait Getter {
    fn t_nodes(&self) -> Vec<Node>;
    fn t_dense_nodes(&self) -> Vec<DenseNode>;
    fn t_ways(&self) -> Vec<Way>;
    fn t_relations(&self) -> Vec<Relation>;
}

impl Getter for PrimitiveBlock {
    fn t_nodes(&self) -> Vec<Node> {
        self.groups().flat_map(|g| g.nodes()).collect()
    }

    fn t_dense_nodes(&self) -> Vec<DenseNode> {
        self.groups().flat_map(|g| g.dense_nodes()).collect()
    }

    fn t_ways(&self) -> Vec<Way> {
        self.groups().flat_map(|g| g.ways()).collect()
    }

    fn t_relations(&self) -> Vec<Relation> {
        self.groups().flat_map(|g| g.relations()).collect()
    }
}

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() < 1.0e-6
}

/// Ensure two vectors have the same values, ignoring their order
fn is_same_unordered(a: &[&str], b: &[String]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

// Compare the content of a HeaderBlock with known values from the test file.
fn check_header_block_content(block: &HeaderBlock, test_file: &TestFile) {
    let res = block.required_features();
    assert!(
        is_same_unordered(test_file.req, res),
        "Required features {res:?} don't match expected {:?}",
        test_file.req
    );
    let res = block.optional_features();
    assert!(
        is_same_unordered(test_file.opt, res),
        "Optional features {res:?} don't match expected {:?}",
        test_file.opt
    );
}

// Compare the content of a PrimitiveBlock with known values from the test file.
fn check_primitive_block_content(block: &PrimitiveBlock) {
    let nodes = block.t_nodes();
    if !nodes.is_empty() {
        assert_eq!(nodes.len(), 3);

        // node 1 lat
        assert!(approx_eq(nodes[1].lat(), 52.11992359584));
        assert_eq!(nodes[1].nano_lat(), 52119923500);
        assert_eq!(nodes[1].decimicro_lat(), 521199235);
        // node 1 lon
        assert!(approx_eq(nodes[1].lon(), 11.62564468943));
        assert_eq!(nodes[1].nano_lon(), 11625644600);
        assert_eq!(nodes[1].decimicro_lon(), 116256446);

        // node 2 lat
        assert!(approx_eq(nodes[2].lat(), 52.11989910567));
        assert_eq!(nodes[2].nano_lat(), 52119899100);
        assert_eq!(nodes[2].decimicro_lat(), 521198991);
        //node 2 lon
        assert!(approx_eq(nodes[2].lon(), 11.63101926915));
        assert_eq!(nodes[2].nano_lon(), 11631019200);
        assert_eq!(nodes[2].decimicro_lon(), 116310192);

        assert_eq!(nodes[0].id(), 105);
        assert_eq!(nodes[1].id(), 106);
        assert_eq!(nodes[2].id(), 108);

        assert_eq!(nodes[0].info().uid(), Some(17));
        assert_eq!(nodes[1].info().uid(), Some(17));
        assert_eq!(nodes[2].info().uid(), Some(17));

        assert!(nodes[0].info().visible());
        assert!(nodes[1].info().visible());
        assert!(nodes[2].info().visible());
    }

    let dense_nodes = block.t_dense_nodes();
    if !dense_nodes.is_empty() {
        assert_eq!(dense_nodes.len(), 3);

        // node 1 lat
        assert!(approx_eq(dense_nodes[1].lat(), 52.11992359584));
        assert_eq!(dense_nodes[1].nano_lat(), 52119923500);
        assert_eq!(dense_nodes[1].decimicro_lat(), 521199235);
        //node 1 lon
        assert!(approx_eq(dense_nodes[1].lon(), 11.62564468943));
        assert_eq!(dense_nodes[1].nano_lon(), 11625644600);
        assert_eq!(dense_nodes[1].decimicro_lon(), 116256446);

        //node 2 lat
        assert!(approx_eq(dense_nodes[2].lat(), 52.11989910567));
        assert_eq!(dense_nodes[2].nano_lat(), 52119899100);
        assert_eq!(dense_nodes[2].decimicro_lat(), 521198991);
        // node 2 lon
        assert!(approx_eq(dense_nodes[2].lon(), 11.63101926915));
        assert_eq!(dense_nodes[2].nano_lon(), 11631019200);
        assert_eq!(dense_nodes[2].decimicro_lon(), 116310192);

        assert_eq!(dense_nodes[0].id, 105);
        assert_eq!(dense_nodes[1].id, 106);
        assert_eq!(dense_nodes[2].id, 108);

        assert_eq!(dense_nodes[0].info().map(|x| x.uid()), Some(17));
        assert_eq!(dense_nodes[1].info().map(|x| x.uid()), Some(17));
        assert_eq!(dense_nodes[2].info().map(|x| x.uid()), Some(17));

        assert_eq!(dense_nodes[0].info().map(|x| x.visible()), Some(true));
        assert_eq!(dense_nodes[1].info().map(|x| x.visible()), Some(true));
        assert_eq!(dense_nodes[2].info().map(|x| x.visible()), Some(true));
    }

    {
        let ways = block.t_ways();
        assert_eq!(ways.len(), 1);
        assert_eq!(ways[0].id(), 107);

        let way_tags = ways[0].tags().collect::<Vec<_>>();
        assert_eq!(way_tags.len(), 2);

        assert!(way_tags.contains(&("building", "yes")));
        assert!(way_tags.contains(&("name", "triangle")));

        let way_refs: Vec<_> = ways[0].refs().collect();
        assert_eq!(way_refs, [105, 106, 108, 105]);
        assert_eq!(ways[0].node_locations().count(), 0);
    }

    {
        let relations = block.t_relations();
        assert_eq!(relations.len(), 1);

        let tags = relations[0].tags().collect::<Vec<_>>();
        assert_eq!(tags.len(), 1);
        assert!(tags.contains(&("rel_key", "rel_value")));

        let members = relations[0].members().collect::<Vec<_>>();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].role().unwrap(), "test_role");
    }
}

#[test]
fn read_blobs() {
    for test_file in TEST_FILE_PATHS {
        let reader = BlobReader::from_path(test_file.path).unwrap();
        let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
        assert_eq!(blobs[1].get_type(), BlobType::OsmData);

        let header = blobs[0].to_headerblock().unwrap();
        check_header_block_content(&header, test_file);

        let primitive_block = blobs[1].to_primitiveblock().unwrap();
        check_primitive_block_content(&primitive_block);
    }
}

#[test]
fn read_mmap_blobs() {
    for test_file in TEST_FILE_PATHS {
        let mmap = unsafe { Mmap::from_path(test_file.path).unwrap() };
        let reader = MmapBlobReader::new(&mmap);

        let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
        assert_eq!(blobs[1].get_type(), BlobType::OsmData);

        if let BlobDecode::OsmHeader(header) = blobs[0].decode().unwrap() {
            check_header_block_content(&header, test_file);
        } else {
            panic!("Unexpected blob type");
        }

        if let BlobDecode::OsmData(primitive_block) = blobs[1].decode().unwrap() {
            check_primitive_block_content(&primitive_block);
        } else {
            panic!("Unexpected blob type");
        }
    }
}

#[test]
fn decode_blob() {
    for test_file in TEST_FILE_PATHS {
        let reader = BlobReader::from_path(test_file.path).unwrap();
        let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
        assert_eq!(blobs[1].get_type(), BlobType::OsmData);

        // Decoding to the wrong blob type should not panic but produce an Err.
        assert!(blobs[0].to_primitiveblock().is_err());
        assert!(blobs[1].to_headerblock().is_err());

        assert!(blobs[0].to_headerblock().is_ok());
        assert!(blobs[1].to_primitiveblock().is_ok());
    }
}

#[test]
fn read_elements() {
    for test_file in TEST_FILE_PATHS {
        let reader = ElementReader::from_path(test_file.path).unwrap();
        let mut elements = 0_usize;

        reader.for_each(|_element| elements += 1).unwrap();

        assert_eq!(elements, 5);
    }
}

#[test]
fn par_read_elements() {
    for test_file in TEST_FILE_PATHS {
        let reader = ElementReader::from_path(test_file.path).unwrap();

        let elements = reader
            .par_map_reduce(|_element| 1, || 0_usize, |a, b| a + b)
            .unwrap();

        assert_eq!(elements, 5);
    }
}

#[test]
fn read_ways_and_deps() {
    for test_file in TEST_FILE_PATHS {
        let mut reader = IndexedReader::from_path(test_file.path).unwrap();

        let mut ways = 0;
        let mut nodes = 0;

        reader
            .read_ways_and_deps(
                |way| way.tags().any(|key_value| key_value == ("building", "yes")),
                |element| {
                    match element {
                        Element::Way(_) => ways += 1,
                        Element::Node(_) => nodes += 1,
                        Element::DenseNode(_) => nodes += 1,
                        Element::Relation(_) => panic!(), // should not occur
                    }
                },
            )
            .unwrap();

        assert_eq!(ways, 1);
        assert_eq!(nodes, 3);
    }
}

#[test]
fn read_history_file() {
    let reader = BlobReader::from_path(HISTORY_FILE_PATH.path).unwrap();
    let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(blobs.len(), 2);
    assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
    assert_eq!(blobs[1].get_type(), BlobType::OsmData);

    let header = blobs[0].to_headerblock().unwrap();
    check_header_block_content(&header, &HISTORY_FILE_PATH);

    let primitive_block = blobs[1].to_primitiveblock().unwrap();
    let nodes = primitive_block.t_dense_nodes();

    assert_eq!(nodes.len(), 2);

    assert!(!nodes[0].info().unwrap().visible());
    assert!(nodes[1].info().unwrap().visible());
}

#[test]
fn read_loc_on_ways_file() {
    let reader = BlobReader::from_path(LOC_ON_WAYS_FILE_PATH.path).unwrap();
    let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(blobs.len(), 3);
    assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
    assert_eq!(blobs[1].get_type(), BlobType::OsmData);
    assert_eq!(blobs[1].get_type(), BlobType::OsmData);

    let header = blobs[0].to_headerblock().unwrap();
    check_header_block_content(&header, &LOC_ON_WAYS_FILE_PATH);

    {
        let primitive_block = blobs[1].to_primitiveblock().unwrap();
        assert_eq!(primitive_block.t_dense_nodes().len(), 0);
        assert_eq!(primitive_block.t_relations().len(), 0);
        let ways = primitive_block.t_ways();
        assert_eq!(ways.len(), 1);
        let way = &ways[0];
        assert_eq!(way.id(), 107);
        let tags = way.tags().collect::<Vec<_>>();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&("building", "yes")));
        assert!(tags.contains(&("name", "triangle")));
        let refs: Vec<_> = way.refs().collect();
        assert_eq!(refs, [105, 106, 108, 105]);
        let nodes: Vec<_> = way.node_locations().collect();
        assert_eq!(nodes.len(), 4);
        // node 0 lat
        assert_approx_eq!(nodes[0].lat(), 52.1224031);
        assert_eq!(nodes[0].nano_lat(), 52122403100);
        assert_eq!(nodes[0].decimicro_lat(), 521224031);
        //node 0 lon
        assert_approx_eq!(nodes[0].lon(), 11.6284017);
        assert_eq!(nodes[0].nano_lon(), 11628401700);
        assert_eq!(nodes[0].decimicro_lon(), 116284017);
        // node 1 lat
        assert_approx_eq!(nodes[1].lat(), 52.11992359584);
        assert_eq!(nodes[1].nano_lat(), 52119923500);
        assert_eq!(nodes[1].decimicro_lat(), 521199235);
        //node 1 lon
        assert_approx_eq!(nodes[1].lon(), 11.62564468943);
        assert_eq!(nodes[1].nano_lon(), 11625644600);
        assert_eq!(nodes[1].decimicro_lon(), 116256446);
        //node 2 lat
        assert_approx_eq!(nodes[2].lat(), 52.11989910567);
        assert_eq!(nodes[2].nano_lat(), 52119899100);
        assert_eq!(nodes[2].decimicro_lat(), 521198991);
        // node 2 lon
        assert_approx_eq!(nodes[2].lon(), 11.63101926915);
        assert_eq!(nodes[2].nano_lon(), 11631019200);
        assert_eq!(nodes[2].decimicro_lon(), 116310192);
        // node 3 lat
        assert_approx_eq!(nodes[3].lat(), nodes[0].lat());
        assert_eq!(nodes[3].nano_lat(), nodes[0].nano_lat());
        assert_eq!(nodes[3].decimicro_lat(), nodes[0].decimicro_lat());
        //node 3 lon
        assert_approx_eq!(nodes[3].lon(), nodes[0].lon());
        assert_eq!(nodes[3].nano_lon(), nodes[0].nano_lon());
        assert_eq!(nodes[3].decimicro_lon(), nodes[0].decimicro_lon());
    }

    {
        let primitive_block = blobs[2].to_primitiveblock().unwrap();
        assert_eq!(primitive_block.t_dense_nodes().len(), 0);
        assert_eq!(primitive_block.t_ways().len(), 0);

        let relations = primitive_block.t_relations();
        assert_eq!(relations.len(), 1);
        let tags = relations[0].tags().collect::<Vec<_>>();
        assert_eq!(tags.len(), 1);
        assert!(tags.contains(&("rel_key", "rel_value")));

        let members = relations[0].members().collect::<Vec<_>>();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].role().unwrap(), "test_role");
    }
}
