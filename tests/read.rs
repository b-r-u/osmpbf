extern crate osmpbf;

use osmpbf::*;

static TEST_FILE_PATHS: [&str; 3] = [
"tests/test.osm.pbf",
"tests/test_nozlib.osm.pbf",
"tests/test_nozlib_nodense.osm.pbf",
];

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() < 1.0e-6
}

// Compare the content of a HeaderBlock with known values from the test file.
fn check_header_block_content(block: &HeaderBlock) {
    for feature in block.required_features() {
        if feature != "OsmSchema-V0.6" && feature != "DenseNodes" {
            panic!("unknown required feature: {}", feature);
        }
    }
    assert_eq!(block.optional_features().len(), 0);
}

// Compare the content of a PrimitiveBlock with known values from the test file.
fn check_primitive_block_content(block: &PrimitiveBlock) {
    let nodes: Vec<_> = block.groups().flat_map(|g| g.nodes()).collect();
    if nodes.len() > 0 {
        assert_eq!(nodes.len(), 3);

        assert!(approx_eq(nodes[1].lat(), 52.11992359584));
        assert!(approx_eq(nodes[1].lon(), 11.62564468943));

        assert!(approx_eq(nodes[2].lat(), 52.11989910567));
        assert!(approx_eq(nodes[2].lon(), 11.63101926915));

        assert_eq!(nodes[0].id(), 105);
        assert_eq!(nodes[1].id(), 106);
        assert_eq!(nodes[2].id(), 108);

        assert_eq!(nodes[0].info().uid(), Some(17));
        assert_eq!(nodes[1].info().uid(), Some(17));
        assert_eq!(nodes[2].info().uid(), Some(17));
    }

    let dense_nodes: Vec<_> = block.groups().flat_map(|g| g.dense_nodes()).collect();
    if dense_nodes.len() > 0 {
        assert_eq!(dense_nodes.len(), 3);

        assert!(approx_eq(dense_nodes[1].lat(), 52.11992359584));
        assert!(approx_eq(dense_nodes[1].lon(), 11.62564468943));

        assert!(approx_eq(dense_nodes[2].lat(), 52.11989910567));
        assert!(approx_eq(dense_nodes[2].lon(), 11.63101926915));

        assert_eq!(dense_nodes[0].id, 105);
        assert_eq!(dense_nodes[1].id, 106);
        assert_eq!(dense_nodes[2].id, 108);

        assert_eq!(dense_nodes[0].uid, 17);
        assert_eq!(dense_nodes[1].uid, 17);
        assert_eq!(dense_nodes[2].uid, 17);
    }

    {
        let ways: Vec<_> = block.groups().flat_map(|g| g.ways()).collect();
        assert_eq!(ways.len(), 1);

        let way_tags = ways[0].tags().collect::<Vec<_>>();
        assert_eq!(way_tags.len(), 2);

        assert!(way_tags.contains(&("building", "yes")));
        assert!(way_tags.contains(&("name", "triangle")));
    }
}

#[test]
fn read() {
    for path in &TEST_FILE_PATHS {
        let reader = BlobReader::from_path(path).unwrap();
        let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
        assert_eq!(blobs[1].get_type(), BlobType::OsmData);

        let header = blobs[0].to_headerblock().unwrap();
        check_header_block_content(&header);

        let primitive_block = blobs[1].to_primitiveblock().unwrap();
        check_primitive_block_content(&primitive_block);
    }
}

#[test]
fn mmap_read() {
    for path in &TEST_FILE_PATHS {
        let mmap = unsafe { Mmap::from_path(path).unwrap() };
        let reader = MmapBlobReader::new(&mmap);

        let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
        assert_eq!(blobs[1].get_type(), BlobType::OsmData);

        if let BlobDecode::OsmHeader(header) = blobs[0].decode().unwrap() {
            check_header_block_content(&header);
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
    for path in &TEST_FILE_PATHS {
        let reader = BlobReader::from_path(path).unwrap();
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
