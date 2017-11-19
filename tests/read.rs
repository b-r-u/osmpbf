extern crate osmpbf;

use osmpbf::*;

static TEST_FILE_PATH: &str = "tests/test.osm.pbf";

fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() < 1.0e-6
}

#[test]
fn read() {
    let reader = BlobReader::from_path(TEST_FILE_PATH).unwrap();

    let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(blobs.len(), 2);

    assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
    assert_eq!(blobs[1].get_type(), BlobType::OsmData);

    {
        let header = blobs[0].to_headerblock().unwrap();
        assert!(header.required_features().contains(&String::from("OsmSchema-V0.6")));
        assert!(header.required_features().contains(&String::from("DenseNodes")));
    }

    {
        let primitive_block = blobs[1].to_primitiveblock().unwrap();

        let nodes = primitive_block.groups().flat_map(|g| g.nodes()).count();
        assert_eq!(nodes, 0);

        {
            let dense_nodes: Vec<_> = primitive_block.groups().flat_map(|g| g.dense_nodes()).collect();
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
            let ways: Vec<_> = primitive_block.groups().flat_map(|g| g.ways()).collect();
            assert_eq!(ways.len(), 1);

            let way_tags = ways[0].tags().collect::<Vec<_>>();
            assert_eq!(way_tags.len(), 2);

            assert!(way_tags.contains(&("building", "yes")));
            assert!(way_tags.contains(&("name", "triangle")));
        }
    }
}

#[test]
fn mmap_read() {
    let mmap = unsafe { Mmap::from_path(TEST_FILE_PATH).unwrap() };
    let reader = MmapBlobReader::new(&mmap);

    let blobs = reader.collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(blobs.len(), 2);

    assert_eq!(blobs[0].get_type(), BlobType::OsmHeader);
    assert_eq!(blobs[1].get_type(), BlobType::OsmData);

    if let BlobDecode::OsmHeader(header) = blobs[0].decode().unwrap() {
        assert!(header.required_features().contains(&String::from("OsmSchema-V0.6")));
        assert!(header.required_features().contains(&String::from("DenseNodes")));
    } else {
        panic!("Unexpected blob type");
    }

    if let BlobDecode::OsmData(primitive_block) = blobs[1].decode().unwrap() {
        let nodes = primitive_block.groups().flat_map(|g| g.nodes()).count();
        assert_eq!(nodes, 0);

        {
            let dense_nodes: Vec<_> = primitive_block.groups().flat_map(|g| g.dense_nodes()).collect();
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
            let ways: Vec<_> = primitive_block.groups().flat_map(|g| g.ways()).collect();
            assert_eq!(ways.len(), 1);

            let way_tags = ways[0].tags().collect::<Vec<_>>();
            assert_eq!(way_tags.len(), 2);

            assert!(way_tags.contains(&("building", "yes")));
            assert!(way_tags.contains(&("name", "triangle")));
        }
    } else {
        panic!("Unexpected blob type");
    }
}
