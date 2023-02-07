package org.apache.seatunnel.connectors.doris;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.seatunnel.connectors.doris.client.HttpHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class RestTest {

    private String feHostPort = "10.192.147.1:9050";
    private String db = "cdc_binlog_zt17606";
    private String tab = "ods_st_lts_address_information_push_test";
    private String username = "root";
    private String password = "root";

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    private void println(Object obj) {
        System.out.println(JSON.toJSONString(obj, JSONWriter.Feature.PrettyFormat));
    }

    @Test
    public void frontends() {
        HttpHelper httpHelper = new HttpHelper();
        String url = String.format("http://%s/rest/v2/manager/node/frontends", feHostPort);
        System.out.println(url);
        try {
            Map<String, String> headers = new HashMap<>();
            headers.put(HttpHeaders.AUTHORIZATION, basicAuthHeader(username, password));
            Map<String, Object> resp = httpHelper.doHttpGet(url, headers);
            println(resp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void _schema() {
        HttpHelper httpHelper = new HttpHelper();
        String url = String.format("http://%s/api/%s/%s/_schema", feHostPort, db, tab);
        System.out.println(url);
        try {
            Map<String, String> headers = new HashMap<>();
            headers.put(HttpHeaders.AUTHORIZATION, basicAuthHeader(username, password));
            Map<String, Object> resp = httpHelper.doHttpGet(url, headers);
            println(resp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
