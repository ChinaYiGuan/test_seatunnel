package org.apache.seatunnel.common.utils;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.filter.SimplePropertyPreFilter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class MultilineJsonFormatUtil {

    public static CvtResp read(String record, boolean isFlat, boolean isType) {
        CvtResp resp = new CvtResp();
        resp.setDataJson(record);
        try {
            if (StringUtils.isNotBlank(record) && JSON.isValidObject(record)) {
                JSONObject jsonObj = JSONObject.parse(record);
                if (jsonObj != null && jsonObj.containsKey(CollectionConstants.JSON_DATA_KEY) && jsonObj.containsKey(CollectionConstants.JSON_META_KEY)) {
                    List<CvtData> cvtDatas = new ArrayList<>();
                    Object cvtDataObj = jsonObj.get(CollectionConstants.JSON_DATA_KEY);
                    if (cvtDataObj instanceof JSONArray) {
                        JSONArray jsonArray = jsonObj.getJSONArray(CollectionConstants.JSON_DATA_KEY);
                        jsonArray.stream()
                                .filter(x -> x instanceof CvtData)
                                .map(x -> (CvtData) x)
                                .forEach(cvtDatas::add);
                    } else if (cvtDataObj instanceof String) {
                        cvtDatas = JSON.parseArray(cvtDataObj.toString(), CvtData.class);
                    }
                    SimplePropertyPreFilter spf = new SimplePropertyPreFilter();
                    if (!isType) {
                        cvtDatas.forEach(x -> x.setType(null));
                        spf.getExcludes().add("type");
                    }
                    String datasJson;
                    if (isFlat) {
                        datasJson = cvtDatas.stream()
                                .map(x -> {
                                    String name = x.getName();
                                    Object value = x.getValue();
                                    JSONObject tmp = new JSONObject();
                                    tmp.put(name, value);
                                    String tmpJson = tmp.toJSONString(JSONWriter.Feature.WriteNulls);
                                    return StringUtils.strip(StringUtils.strip(tmpJson, "{"), "}");
                                }).collect(Collectors.joining(",", "{", "}"));

                    } else {
                        datasJson = JSON.toJSONString(cvtDatas, spf, JSONWriter.Feature.WriteNulls);
                    }

                    CvtMeta cvtMeta = new CvtMeta();
                    Object cvtMetaObj = jsonObj.get(CollectionConstants.JSON_META_KEY);
                    if (cvtMetaObj instanceof JSONObject) {
                        cvtMeta = jsonObj.getObject(CollectionConstants.JSON_META_KEY, CvtMeta.class);
                    } else if (cvtMetaObj instanceof String) {
                        cvtMeta = JSON.parseObject(cvtMetaObj.toString(), CvtMeta.class);
                    }
                    String metaJson = JSON.toJSONString(cvtMeta, JSONWriter.Feature.WriteNulls);

                    return new CvtResp(
                            cvtDatas.toArray(new CvtData[0]),
                            datasJson,
                            cvtMeta,
                            metaJson
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return resp;
    }


    public static String readStr(String record, boolean isFlat, boolean isType) {
        CvtResp resp = read(record, isFlat, isType);
        return cvtResp(resp);
    }

    private static String cvtResp(CvtResp resp) {
        if (resp != null) {
            JSONObject rtJson = new JSONObject();
            rtJson.put("data", resp.getData());
            rtJson.put("data", resp.getData());
            rtJson.put("meta", resp.getMeta());
            rtJson.put("meta", resp.getMeta());
            return rtJson.toJSONString(JSONWriter.Feature.WriteNulls);
        }
        return null;
    }

    /**
     * @param datas      《name,type,value》
     * @param identifier
     * @return 《data,meta》
     */
    public static CvtResp writer(CvtData[] datas, String identifier) {
        if (datas != null && datas.length > 0) {
            final String dataJson = JSON.toJSONString(datas, JSONWriter.Feature.WriteNulls);
            String identifierPk = StringUtils.isBlank(identifier) ? UUID.randomUUID().toString().replace("-", "") : identifier;
            LocalDateTime time = LocalDateTime.now();
            List<String> names = Arrays.stream(datas).map(CvtData::getName).collect(Collectors.toList());
            List<String> types = Arrays.stream(datas).map(CvtData::getType).collect(Collectors.toList());
            CvtMeta cvtMeta = new CvtMeta(identifierPk, time, names, types);
            final String metaJson = JSON.toJSONString(new CvtMeta(identifierPk, time, names, types), JSONWriter.Feature.WriteNulls);
            return new CvtResp(datas, dataJson, cvtMeta, metaJson);
        }
        return new CvtResp();
    }

    public static String writerStr(CvtData[] datas, String identifier) {
        CvtResp resp = writer(datas, identifier);
        return cvtResp(resp);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CvtData {
        private String name;
        private String type;
        private Object value;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CvtMeta {
        private String identifier;
        private LocalDateTime time;
        private List<String> names;
        private List<String> types;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CvtResp {
        private CvtData[] data;
        private String dataJson;
        private CvtMeta meta;
        private String metaJson;
    }

}
