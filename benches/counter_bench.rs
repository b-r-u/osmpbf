use criterion::{criterion_group, criterion_main, Criterion};
use osmpbf::{Element, ElementReader};
use std::env;

criterion_group!(benches, bench_count);
criterion_main!(benches);

fn bench_count(c: &mut Criterion) {
    let file = env!(
        "OSMPBF_BENCH_FILE",
        "Must specify OSMPBF_BENCH_FILE env var when compiling this benchmark"
    );

    #[cfg(feature = "rust-zlib")]
    println!("Using rust-zlib (miniz_oxide)");
    #[cfg(feature = "zlib")]
    println!("Using zlib");
    #[cfg(feature = "zlib-ng")]
    println!("Using zlib-ng");

    c.bench_function(format!("Benchmarking using {file}").as_str(), |b| {
        b.iter(|| {
            let path = std::path::Path::new(file);
            let reader = ElementReader::from_path(path).unwrap();
            reader
                .par_map_reduce(
                    |element| match element {
                        Element::Node(_) | Element::DenseNode(_) => (1, 0, 0),
                        Element::Way(_) => (0, 1, 0),
                        Element::Relation(_) => (0, 0, 1),
                    },
                    || (0u64, 0u64, 0u64),
                    |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2),
                )
                .unwrap()
        })
    });
}
