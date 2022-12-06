use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use chrono::{Local, TimeZone};
use flate2::Compression;
use rand::{thread_rng, Rng};
use sha2::{Digest, Sha256};

struct RawLine<'a> {
    remote_addr: &'a str,
    time_local: &'a str,
    request: &'a str,
    status: &'a str,
    body_bytes_send: &'a str,
    http_referer: &'a str,
    http_user_agent: &'a str,
    #[allow(dead_code)]
    http_x_forwarded_for: &'a str,
    http_cookie: &'a str,
    #[allow(dead_code)]
    request_time: &'a str,
    #[allow(dead_code)]
    upstream_response_time: &'a str,
    #[allow(dead_code)]
    upstream_addr: &'a str,
    cookie_coresessionid: &'a str,
    #[allow(dead_code)]
    http_x_maa_mark: &'a str,
    #[allow(dead_code)]
    http_cdn_src_ip: &'a str,
    #[allow(dead_code)]
    http_x_maa_chainmark: &'a str,
}

// cvs文件字段
// id, client_ip, request_time, request_uri, response_status,
// request_body, content_length, content_type, user_agent, method,
// referer, cookie, origin, host, session_id,
// channel, h24, proxied, request_status, is_update,
// uniform_uri, params, headers, finger, file,
// region, action, browser, platform, static_url

// raw文件字段
// remote_addr: 10.14.64.111
// time_local: 31/Dec/2018:00:38:32 +0800
// request: POST /szair_B2C/login/loginOrOut.action HTTP/1.1
// status: 200
// body_bytes_send: 30
// http_referer: http://www.shenzhenair.com/
// http_user_agent: Mozilla/5.0 (compatible; MSIE 2.0; Win32 9949; 2.0 Gecko/68.731; Windows NT 11.0; Trident/4.0)
// http_x_forwarded_for: 59.52.100.194, 183.2.215.19
// http_cookie: AlteonP=BdxAKW9ADgpSvulonVFhPA$$; sign_cookie=67bc38b0d8487c44139b9315f396fb0e; sign_flight=561083face2fc79f80897c41b1735483; CoreSessionId=96770a8e9ad8e169db75a85f40f66a3f980f56d4d21eb47a; _g_sign=1b2671f66179b5bcb61967f2e68afc0a
// request_time: 0.022
// upstream_response_time: 0.012
// upstream_addr: 10.14.79.101:80
// cookie_coresessionid: 96770a8e9ad8e169db75a85f40f66a3f980f56d4d21eb47a

pub fn raw_to_csv(input_file: &Path, output_file: &Path, thread_counter: Arc<Mutex<usize>>) {
    let output_file = File::create(output_file).unwrap();
    let mut gz_encoder = flate2::write::GzEncoder::new(output_file, Compression::default());
    let input_file = File::open(input_file).unwrap();
    let gz_decoder = flate2::read::GzDecoder::new(input_file);
    let buf_reader = BufReader::new(gz_decoder);
    for line in buf_reader.lines() {
        let line = line.unwrap();
        let (remote_addr, line) = line.split_once(" ").unwrap();
        let (_, line) = line.split_once("- [").unwrap();
        let (time_local, line) = line.split_once("] \"").unwrap();
        let (request, line) = line.split_once("\" ").unwrap();
        let (status, line) = line.split_once(" ").unwrap();
        let (body_bytes_send, line) = line.split_once(" \"").unwrap();
        let (http_referer, line) = line.split_once("\" \"").unwrap();
        let (http_user_agent, line) = line.split_once("\" \"").unwrap();
        let (http_x_forwarded_for, line) = line.split_once("\" \"").unwrap();
        let (http_cookie, line) = line.split_once("\" \"").unwrap();
        let (request_time, line) = line.split_once("\" \"").unwrap();
        let (upstream_response_time, line) = line.split_once("\" \"").unwrap();
        let (upstream_addr, line) = line.split_once("\" \"").unwrap();
        let (cookie_coresessionid, _) = line.split_once("\"").unwrap();
        let raw_line = RawLine {
            remote_addr,
            time_local,
            request,
            status,
            body_bytes_send,
            http_referer,
            http_user_agent,
            http_x_forwarded_for,
            http_cookie,
            request_time,
            upstream_response_time,
            upstream_addr,
            cookie_coresessionid,
            http_x_maa_mark: "",
            http_cdn_src_ip: "",
            http_x_maa_chainmark: "",
        };
        let id = raw_line.cookie_coresessionid;
        let client_ip = raw_line.remote_addr;
        let request_time = raw_line.time_local;
        let (method, line) = raw_line.request.split_once(" ").unwrap();
        let (request_uri, _) = line.split_once(" ").unwrap();
        let response_status = raw_line.status;
        let request_body = "-";
        let content_length = raw_line.body_bytes_send;
        let content_type = "-";
        let user_agent = raw_line.http_user_agent;
        let referer = raw_line.http_referer;
        let cookie = raw_line.http_cookie;
        let origin = "-";
        let host = "-";
        let session_id = "-";
        let channel = "-";
        let h24 = "-";
        let proxied = "-";
        let request_status = "-";
        let is_update = "-";
        let uniform_uri = "-";
        let params = "-";
        let headers = "-";
        let finger = hex::encode(Sha256::digest(format!("xt{}", client_ip)));
        let file = "-";
        let region = "-";
        let action = "-";
        let browser = "-";
        let platform = "-";
        let static_url = "-";

        let cvs = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            id, client_ip, request_time, request_uri, response_status,
            request_body, content_length, content_type, user_agent, method,
            referer, cookie, origin, host, session_id,
            channel, h24, proxied, request_status, is_update,
            uniform_uri, params, headers, finger, file,
            region, action, browser, platform, static_url
        );
        gz_encoder.write_all(cvs.as_bytes()).unwrap();
    }
    gz_encoder.finish().unwrap();
    let mut thread_count = thread_counter.lock().unwrap();
    *thread_count -= 1;
    drop(thread_count);
}

#[allow(dead_code)]
fn clean_2019() {
    let csv_2019 = File::open("./input/20190103.csv").unwrap();
    let csv_buf_reader = BufReader::new(csv_2019);
    let mut output_file = File::create("./output/2019.csv").unwrap();
    let mut rng = thread_rng();
    let header = "ORDER_NO,ORDER_TIME,ORDER_STATUS,USER_IP,MEMBER_ID,RANGE_TYPE,ORG_CITY,DST_CITY,CONTACTS_NAME,ID,PLAT_ID,PRICE,START_TIME\n";
    output_file.write_all(header.as_bytes()).unwrap();
    for line in csv_buf_reader.lines() {
        let line = line.unwrap();
        let line = line.replace("\"", "");
        let plat_id = match rng.gen_range(0..3) {
            0 => "Android_2014",
            1 => "iOS_2014",
            _ => "",
        };
        let price = rng.gen_range(200..3000);
        let line_vec: Vec<&str> = line.split(",").collect();
        let order_time = line_vec[1];
        let (day, order_time) = order_time.split_once("/").unwrap();
        let (month, order_time) = order_time.split_once("/").unwrap();
        let (year, order_time) = order_time.split_once(" ").unwrap();
        let (hour, order_time) = order_time.split_once(":").unwrap();
        let (minute, second) = order_time.split_once(":").unwrap();
        let order_time = Local
            .with_ymd_and_hms(
                year.parse().unwrap(),
                month.parse().unwrap(),
                day.parse().unwrap(),
                hour.parse().unwrap(),
                minute.parse().unwrap(),
                second.parse().unwrap(),
            )
            .unwrap();
        let order_time = order_time.timestamp() + rng.gen_range(60 * 60 * 15..60 * 60 * 100);
        let order_time = Local.timestamp_opt(order_time, 0).unwrap();
        let order_time = order_time.format("%Y-%m-%d %H:%M").to_string();
        let line = format!("{},{},{},{}\n", line, plat_id, price, order_time);
        output_file.write_all(line.as_bytes()).unwrap();
    }

    let txt_2019 = File::open("./input/20191101.txt").unwrap();
    let txt_buf_reader = BufReader::new(txt_2019);
    for line in txt_buf_reader.lines() {
        let line = line.unwrap();
        let line = line.replace("\"", "");
        if line.starts_with("ORDER_NO") {
            continue;
        }
        let line_vec: Vec<&str> = line.split("\t").collect();
        let user_ip = line_vec[3];
        let user_ip = {
            if user_ip.len() > 0 {
                let user_ip: Vec<&str> = user_ip.split(",").collect();
                user_ip[0]
            } else {
                ""
            }
        };
        let price = rng.gen_range(200..3000);
        let order_time = line_vec[1];
        let (day, order_time) = order_time.split_once("/").unwrap();
        let (month, order_time) = order_time.split_once("/").unwrap();
        let (year, order_time) = order_time.split_once(" ").unwrap();
        let (hour, order_time) = order_time.split_once(":").unwrap();
        let (minute, second) = order_time.split_once(":").unwrap();
        let order_time = Local
            .with_ymd_and_hms(
                year.parse().unwrap(),
                month.parse().unwrap(),
                day.parse().unwrap(),
                hour.parse().unwrap(),
                minute.parse().unwrap(),
                second.parse().unwrap(),
            )
            .unwrap();
        let order_time = order_time.timestamp() + rng.gen_range(60 * 60 * 15..60 * 60 * 100);
        let order_time = Local.timestamp_opt(order_time, 0).unwrap();
        let order_time = order_time.format("%Y-%m-%d %H:%M").to_string();
        let line = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            line_vec[0],
            line_vec[1],
            line_vec[2],
            user_ip,
            line_vec[4],
            line_vec[5],
            line_vec[6],
            line_vec[7],
            line_vec[8],
            line_vec[9],
            line_vec[10],
            price,
            order_time
        );
        output_file.write_all(line.as_bytes()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread::{self, sleep},
        time::Duration,
    };

    use sha2::{Digest, Sha256};

    use super::clean_2019;

    #[test]
    fn it_works() {
        let str = "I miss you";
        let hash = Sha256::digest(str);
        println!("{} hash: {:?}", str, hex::encode(hash));
        let num_cpus = num_cpus::get();
        println!("num_cpus: {}", num_cpus);
        let thread_counter = Arc::new(Mutex::new(0));
        for _ in 0..100 {
            let thread_counter = Arc::clone(&thread_counter);
            loop {
                let mut thread_count = thread_counter.lock().unwrap();
                if *thread_count > 5 {
                    drop(thread_count);
                    sleep(Duration::from_secs(1));
                } else {
                    *thread_count += 1;
                    drop(thread_count);
                    break;
                }
            }
            thread::spawn(move || thread_worker(thread_counter));
        }
    }

    fn thread_worker(thread_counter: Arc<Mutex<i32>>) {
        println!("thread worker is working...");
        sleep(Duration::from_secs(3));
        let mut thread_count = thread_counter.lock().unwrap();
        *thread_count -= 1;
        println!("thread worker is finished");
    }
    #[test]
    fn test_clean_2019() {
        clean_2019();
    }
}
