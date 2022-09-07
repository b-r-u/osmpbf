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

    // Note that both libz and libz-ng can be enabled at the same time.
    // In this case libz will use the ng version internally.

    #[cfg(feature = "system-libz")]
    println!("Using ZlibDecoder...");
    #[cfg(feature = "system-libz-ng")]
    println!("Using ZstdDecoder with NG...");
    #[cfg(not(any(feature = "system-libz", feature = "system-libz-ng")))]
    println!("Using DeflateDecoder...");

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
