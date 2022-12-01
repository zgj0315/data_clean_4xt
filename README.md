# data_clean_4xt
数据清洗及结构化

# 说明
## raw文件中缺少字段
http_x_maa_mark, http_cdn_src_ip, http_x_maa_chainmark

## csv文件中缺少字段（其中直接赋值"-"的无法取到值）
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
