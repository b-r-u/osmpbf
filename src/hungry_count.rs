extern crate osmpbf;
extern crate crossbeam;

use osmpbf::*;
use std::fs::File;
use std::sync::mpsc::sync_channel;


enum WorkerMessage<'a> {
    PleaseStop,
    DoBlob(MmapBlob<'a>),
}

fn stats(file: &File) {
    let mmap = Mmap::from_file(file).unwrap();

    crossbeam::scope(|scope| {
        let mut iter = MmapBlobIter::new(&mmap);

        let num_threads = 4_usize;
        let mut chans = Vec::with_capacity(num_threads);

        let (result_tx, result_rx) = sync_channel::<(usize, usize)>(0);

        for thread_id in 0..num_threads {
            let result_tx = result_tx.clone();

            let (request_tx, request_rx) = sync_channel::<WorkerMessage>(0);
            chans.push(request_tx);

            scope.spawn(move || {
                loop {
                    match request_rx.recv().unwrap() {
                        WorkerMessage::PleaseStop => return,
                        WorkerMessage::DoBlob(mmap_blob) => {
                            let count = if let BlobDecode::OsmData(block) = mmap_blob.decode() {
                                block.groups()
                                     .flat_map(|g| g.dense_nodes())
                                     .count()
                            } else {
                                0
                            };
                            match result_tx.send((thread_id, count)) {
                                Ok(_) => {},
                                Err(_) => {
                                    println!("Thread id {}, Err", thread_id);
                                    return;
                                },
                            };
                        },
                    };
                }
            });
        }

        //TODO fix dead lock when number of blobs is less than number of threads
        for (thread_id, mmap_blob) in iter.by_ref().take(num_threads).enumerate() {
            chans[thread_id].send(WorkerMessage::DoBlob(mmap_blob)).unwrap();
        }

        let mut stopped = 0;
        let mut nodes = 0;
        loop {
            let (thread_id, count) = result_rx.recv().unwrap();
            nodes += count;

            if let Some(mmap_blob) = iter.next() {
                match chans[thread_id].send(WorkerMessage::DoBlob(mmap_blob)) {
                    Ok(_) => {},
                    Err(_) => {println!("Err");},
                };
            } else {
                match chans[thread_id].send(WorkerMessage::PleaseStop) {
                    Ok(_) => {},
                    Err(_) => {
                    }
                };
                stopped += 1;
                if stopped == num_threads {
                    break;
                }
            }
        }

        println!("Number of nodes: {}", nodes);
    });
}

fn main() {
    let path = std::env::args_os().nth(1).unwrap();
    let f = std::fs::File::open(path).unwrap();

    stats(&f);
}
