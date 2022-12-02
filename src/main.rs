use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    thread::{self, sleep},
    time::Duration,
};

use lib::raw_to_csv;

mod lib;
fn main() {
    let format = tracing_subscriber::fmt::format()
        .with_level(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true);
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .event_format(format)
        .init();

    // 留一个核心
    let num_cpus = num_cpus::get() - 1;
    let thread_counter = Arc::new(Mutex::new(0));

    let path = Path::new("./input");
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_file() {
                let file_name = path.to_str().unwrap();
                let (_, file_name) = file_name.rsplit_once("/").unwrap();
                if file_name.starts_with("nginx-access") && file_name.ends_with(".gz") {
                    let file_name = file_name.replace(".gz", ".csv.gz");
                    let file_name = format!("./output/{}", file_name);
                    let output_file = Path::new(&file_name).to_path_buf();
                    log::info!(
                        "{:?} to {:?}",
                        path.file_name().unwrap(),
                        output_file.file_name().unwrap()
                    );
                    let thread_counter = Arc::clone(&thread_counter);
                    loop {
                        let mut thread_count = thread_counter.lock().unwrap();
                        if *thread_count >= num_cpus {
                            drop(thread_count);
                            sleep(Duration::from_millis(100));
                        } else {
                            *thread_count += 1;
                            drop(thread_count);
                            break;
                        }
                    }
                    thread::spawn(move || raw_to_csv(&path, &output_file, thread_counter));
                }
            }
        }
    }
}
