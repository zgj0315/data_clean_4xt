use std::{
    fs::File,
    io::{BufRead, BufReader},
};

#[allow(dead_code)]
struct RawLine {
    remote_addr: String,
    time_local: String,
    request: String,
    status: String,
    body_bytes_send: String,
    http_referer: String,
    http_user_agent: String,
    http_x_forwarded_for: String,
    http_cookie: String,
    request_time: String,
    upstream_response_time: String,
    upstream_addr: String,
    cookie_coresessionid: String,
    http_x_maa_mark: String,
    http_cdn_src_ip: String,
    http_x_maa_chainmark: String,
}
pub fn read_file_by_line() {
    let file = File::open("./data/nginx-access_107.log-20190101").unwrap();
    let buf_reader = BufReader::new(file);
    for line in buf_reader.lines() {
        let line = line.unwrap();
        let (remote_addr, line) = line.split_once(" ").unwrap();
        println!("remote_addr: {}", remote_addr);
        let (_, line) = line.split_once("- [").unwrap();
        let (time_local, line) = line.split_once("] \"").unwrap();
        println!("time_local: {}", time_local);
        let (request, line) = line.split_once("\" ").unwrap();
        println!("request: {}", request);
        let (status, line) = line.split_once(" ").unwrap();
        println!("status: {}", status);
        let (body_bytes_send, line) = line.split_once(" \"").unwrap();
        println!("body_bytes_send: {}", body_bytes_send);
        let (http_referer, line) = line.split_once("\" \"").unwrap();
        println!("http_referer: {}", http_referer);
        let (http_user_agent, line) = line.split_once("\" \"").unwrap();
        println!("http_user_agent: {}", http_user_agent);
        let (http_x_forwarded_for, line) = line.split_once("\" \"").unwrap();
        println!("http_x_forwarded_for: {}", http_x_forwarded_for);
        let (http_cookie, line) = line.split_once("\" \"").unwrap();
        println!("http_cookie: {}", http_cookie);
        let (request_time, line) = line.split_once("\" \"").unwrap();
        println!("request_time: {}", request_time);
        let (upstream_response_time, line) = line.split_once("\" \"").unwrap();
        println!("upstream_response_time: {}", upstream_response_time);
        let (upstream_addr, line) = line.split_once("\" \"").unwrap();
        println!("upstream_addr: {}", upstream_addr);
        let (cookie_coresessionid, _) = line.split_once("\"").unwrap();
        println!("cookie_coresessionid: {}", cookie_coresessionid);
    }
}
