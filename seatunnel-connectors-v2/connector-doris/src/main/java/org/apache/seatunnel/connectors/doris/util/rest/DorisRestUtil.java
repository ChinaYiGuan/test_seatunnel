package org.apache.seatunnel.connectors.doris.util.rest;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.seatunnel.connectors.doris.client.HttpHelper;
import org.apache.seatunnel.connectors.doris.domain.rest.Resp;
import org.apache.seatunnel.connectors.doris.domain.rest.SchemaResp;
import org.apache.seatunnel.connectors.doris.util.TryUtil;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DorisRestUtil implements Serializable {
    private String host;
    private int port;
    private String user;
    private String pass;

    private final int TRY_COUNT = 3;

    private final long TRY_TIMEMS = 10_000;

    private final String urlPrefix;

    public DorisRestUtil(String host, int port, String user, String pass) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pass = pass;
        this.urlPrefix = String.format("http://%s:%s", host, port);
    }

    private <T> T jsonConvertObj(Map<String, Object> in, Class<T> clazz) {
        String bodyStr = JSON.toJSONString(in, JSONWriter.Feature.WriteNulls);
        Resp resp = JSON.parseObject(bodyStr, Resp.class);
        Object dataObj = resp.getData();
        if (dataObj != null && "success".equals(resp.getMsg())) {
            T data = JSON.parseObject(dataObj.toString(), clazz);
            return data;
        }
        log.error("http resp json convert err:{}", in.getOrDefault("data", "not data key"));
        return null;
    }

    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    public SchemaResp querySchema(String tableFullName) {
        return TryUtil.invoke((x) -> {
            HttpHelper httpHelper = new HttpHelper();
            String url = urlPrefix + String.format("/api/%s/%s/_schema", StringUtils.substringBefore(tableFullName, "."), StringUtils.substringAfterLast(tableFullName, "."));
            Map<String, String> headers = new HashMap<>();
            if (StringUtils.isNotBlank(pass) && StringUtils.isNotBlank(user))
                headers.put(HttpHeaders.AUTHORIZATION, basicAuthHeader(user, pass));
            Map<String, Object> resp = httpHelper.doHttpGet(url, headers);
            return jsonConvertObj(resp, SchemaResp.class);
        }, e -> {
        }, TRY_COUNT, TRY_TIMEMS, Collections.emptyList());
    }


}
