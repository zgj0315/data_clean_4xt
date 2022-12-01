use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

use zip::write::FileOptions;

struct RawLine<'a> {
    remote_addr: &'a str,
    time_local: &'a str,
    request: &'a str,
    status: &'a str,
    body_bytes_send: &'a str,
    http_referer: &'a str,
    http_user_agent: &'a str,
    http_x_forwarded_for: &'a str,
    http_cookie: &'a str,
    request_time: &'a str,
    upstream_response_time: &'a str,
    upstream_addr: &'a str,
    cookie_coresessionid: &'a str,
    http_x_maa_mark: &'a str,
    http_cdn_src_ip: &'a str,
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

pub fn raw_to_csv() {
    let zip_file = File::create("./data/20190101_csv.zip").unwrap();
    let mut zip_writer = zip::ZipWriter::new(zip_file);
    let options = FileOptions::default()
        .compression_method(zip::CompressionMethod::Bzip2)
        .unix_permissions(0o400);
    zip_writer.start_file("20190101.cvs", options).unwrap();
    let file = File::open("./data/nginx-access_107.log-20190101.gz").unwrap();
    let file = flate2::read::GzDecoder::new(file);
    let buf_reader = BufReader::new(file);
    for line in buf_reader.lines() {
        let line = line.unwrap();
        let (remote_addr, line) = line.split_once(" ").unwrap();
        // println!("remote_addr: {}", remote_addr);
        let (_, line) = line.split_once("- [").unwrap();
        let (time_local, line) = line.split_once("] \"").unwrap();
        // println!("time_local: {}", time_local);
        let (request, line) = line.split_once("\" ").unwrap();
        // println!("request: {}", request);
        let (status, line) = line.split_once(" ").unwrap();
        // println!("status: {}", status);
        let (body_bytes_send, line) = line.split_once(" \"").unwrap();
        // println!("body_bytes_send: {}", body_bytes_send);
        let (http_referer, line) = line.split_once("\" \"").unwrap();
        // println!("http_referer: {}", http_referer);
        let (http_user_agent, line) = line.split_once("\" \"").unwrap();
        // println!("http_user_agent: {}", http_user_agent);
        let (http_x_forwarded_for, line) = line.split_once("\" \"").unwrap();
        // println!("http_x_forwarded_for: {}", http_x_forwarded_for);
        let (http_cookie, line) = line.split_once("\" \"").unwrap();
        // println!("http_cookie: {}", http_cookie);
        let (request_time, line) = line.split_once("\" \"").unwrap();
        // println!("request_time: {}", request_time);
        let (upstream_response_time, line) = line.split_once("\" \"").unwrap();
        // println!("upstream_response_time: {}", upstream_response_time);
        let (upstream_addr, line) = line.split_once("\" \"").unwrap();
        // println!("upstream_addr: {}", upstream_addr);
        let (cookie_coresessionid, _) = line.split_once("\"").unwrap();
        // println!("cookie_coresessionid: {}", cookie_coresessionid);
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
        let finger = "-";
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
        zip_writer.write_all(cvs.as_bytes()).unwrap();
    }
    zip_writer.finish().unwrap();
}
