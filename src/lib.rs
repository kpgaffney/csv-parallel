use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use rayon::ThreadPoolBuilder;

pub fn partition_csv<P>(input_path: P, num_threads: usize, buf_size: usize)
where
    P: AsRef<Path>,
{
    let input_file = File::open(input_path).unwrap();
    let mut reader = BufReader::new(input_file);

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();

    // This HashMap will hold a String for each partition of the input file.
    let global_partitions = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let mut buf = vec![0; buf_size];

        // Try to fill the buffer with bytes from the input file. If we read zero bytes, we assume
        // that we have reached the end of the file.
        let mut bytes_read = reader.read(&mut buf).unwrap();
        if bytes_read == 0 {
            break;
        }

        // Read until the next newline character. Truncate the buffer to the appropriate length.
        bytes_read += reader.read_until(b'\n', &mut buf).unwrap();
        buf.truncate(bytes_read);

        // Process each chunk in parallel.
        let global_partitions_clone = Arc::clone(&global_partitions);
        thread_pool.install(move || {
            let chunk = String::from_utf8(buf).unwrap();

            // This HashMap will hold a String for each partition of the chunk.
            let mut local_partitions = HashMap::new();

            for line in chunk.split_inclusive('\n') {
                if !line.is_empty() {
                    // Get the partition index. We assume that the partition index is the first
                    // value in the row.
                    let index = line.split_once(',').unwrap().0.parse::<usize>().unwrap();

                    // Append the row to the corresponding partition.
                    local_partitions
                        .entry(index)
                        .or_insert(String::new())
                        .push_str(line);
                }
            }

            // Merge the local partitions into the global partitions.
            let mut global_partitions_guard = global_partitions_clone.lock().unwrap();
            for (index, lines) in local_partitions {
                global_partitions_guard
                    .entry(index)
                    .or_insert(String::new())
                    .push_str(&lines);
            }
        });
    }

    // Write the results to files.
    for (index, lines) in &*global_partitions.lock().unwrap() {
        fs::create_dir_all("result").unwrap();
        let mut output_file = File::options()
            .create(true)
            .write(true)
            .open(format!("result/p{}.csv", index))
            .unwrap();
        output_file.write_all(lines.as_bytes()).unwrap();
    }
}
