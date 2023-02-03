package org.apache.seatunnel.datasource.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Data
@Slf4j
public class HttpHelper {

    @SneakyThrows
    private static HttpURLConnection getConnection(String urlStr, String method, Map<String, String> header) {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod((method == null ? "GET" : method).toUpperCase());

        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);
        conn.setUseCaches(false);
        conn.setRequestProperty("Connection", "Keep-Alive");

        if (header != null && header.size() > 1)
            header.forEach(conn::setRequestProperty);

        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    @SneakyThrows
    public static HttpR doHttp(String urlStr, String method, Map<String, String> header, String body) {
        log.info("doHttp req -> urlStr:{}, method:{}, header size:{}, body size；{}", urlStr, method, header != null ? header.size() : 0, body == null ? 0 : body.getBytes(StandardCharsets.UTF_8).length);
        HttpURLConnection conn = getConnection(urlStr, method, header);
        if (body != null)
            try (BufferedOutputStream bos = new BufferedOutputStream(conn.getOutputStream())) {
                bos.write(body.getBytes(StandardCharsets.UTF_8));
            }
        int status = conn.getResponseCode();
        String respMsg = conn.getResponseMessage();
        log.info("doHttp resp -> urlStr:{}, method:{}, respCode:{}, respMsg；{}", urlStr, method, status, respMsg);
        StringBuilder respSb = new StringBuilder();
        try (InputStream is = (InputStream) conn.getContent();
             InputStreamReader isr = new InputStreamReader(is);
             BufferedReader br = new BufferedReader(isr);
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                respSb.append(line);
            }
        }
        return new HttpR(status, respMsg,respSb.toString());
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class HttpR {
        private int code;
        private String msg;
        private String body;
        public boolean isOK(int code) {
            return code == this.code;
        }

    }

}
