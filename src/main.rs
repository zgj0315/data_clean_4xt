use lib::raw_to_csv;

mod lib;
fn main() {
    let input_file = "nginx-access_107.log-20190101.gz";
    let output_file = "20190101_csv.gz";
    raw_to_csv(input_file, output_file);
}
