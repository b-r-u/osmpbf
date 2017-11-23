#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate osmpbf;

fuzz_target!(|data: &[u8]| {
    let cursor = std::io::Cursor::new(data);
    let reader = osmpbf::ElementReader::new(cursor);

    let mut elements = 0;
    let _ = reader.for_each(|_| {
        elements += 1;
    });
});
