use std::{fs, path::Path};

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
                    let output_file = Path::new(&file_name);
                    log::info!(
                        "{:?} to {:?}",
                        path.file_name().unwrap(),
                        output_file.file_name().unwrap()
                    );
                    raw_to_csv(&path, output_file);
                }
            }
        }
    }
}
