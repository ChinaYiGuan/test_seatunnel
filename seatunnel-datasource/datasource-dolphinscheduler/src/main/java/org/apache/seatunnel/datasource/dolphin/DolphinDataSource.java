package org.apache.seatunnel.datasource.dolphin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.datasource.BaseDataSource;
import org.apache.seatunnel.common.utils.EnvUtil;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.datasource.util.DesUtil;
import org.apache.seatunnel.datasource.util.HttpHelper;
import org.apache.seatunnel.shade.com.typesafe.config.*;

import java.time.Instant;
import java.util.Optional;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/*
{
    "code":0,
    "msg":"success",
    "data":{
        "totalList":[
            {
                "id":12,
                "userId":2,
                "userName":"zt22790",
                "name":"pro_doris-112.14",
                "note":"",
                "type":"MYSQL",
                "connectionParams":"{
                    \"user\":\"etl\",
                    \"password\":\"******\",
                    \"address\":\"jdbc:mysql://10.192.112.14:9030\",
                    \"database\":\"tpcds\",
                    \"jdbcUrl\":\"jdbc:mysql://10.192.112.14:9030/tpcds\",
                    \"driverClassName\":\"com.mysql.cj.jdbc.Driver\",
                    \"validationQuery\":\"select 1\",
                    \"other\":\"hostname=10.192.112.14&database=tpcds&driver=com.mysql.cj.jdbc.Driver&fenodes=10.192.112.14:9050&port=9030&tinyInt1isBit=false&url=jdbc:mysql://10.192.112.14:9030/tpcds&TreatTinyAsBoolean=false&exttype=mysql&status=1&\",
                    \"props\":{
                        \"hostname\":\"10.192.112.14\",
                        \"database\":\"tpcds\",
                        \"driver\":\"com.mysql.cj.jdbc.Driver\",
                        \"fenodes\":\"10.192.112.14:9050\",
                        \"port\":\"9030\",
                        \"tinyInt1isBit\":\"false\",
                        \"url\":\"jdbc:mysql://10.192.112.14:9030/tpcds\",
                        \"TreatTinyAsBoolean\":\"false\",
                        \"exttype\":\"mysql\",
                        \"status\":\"1\"
                    }
                }",
                "createTime":"2022-09-27 10:09:33",
                "updateTime":"2022-12-07 12:34:57"
            }
        ],
        "total":1,
        "totalPage":1,
        "pageSize":20,
        "currentPage":1,
        "start":0
    },
    "failed":false,
    "success":true
}
 */
@Slf4j
@AutoService(BaseDataSource.class)
public class DolphinDataSource implements BaseDataSource {
    private static String pattern = "\\$\\{(.*?)}";

    private static final String ENV_DS_HOSTPORT_KEY = "dataSourceHost";
    private static final String ENV_DS_TOKEN_KEY = "dataSourceToken";

    private static final String ENV_SHOW_DS_INFO_KEY = "is_show_parseDs";
    private static final String URL_TEMPLATE = "HOST_PORT/dolphinscheduler/datasources?pageNo=1&pageSize=20&searchVal=DS_NAME";

    private final Map<String, Object> parseDataSourceMap = new HashMap<>();
    private final String cfgDataSourcePath = "datasource";
    private final String cfgOutDataSourcePath = "__out_" + cfgDataSourcePath;

    private final Map<String, List<String>> dataSourceMapping = new HashMap<String, List<String>>() {
        {
            put("user", Arrays.asList("username", "user"));
            put("password", Arrays.asList("pass", "password"));
            put("driverClassName", Arrays.asList("driverClassName", "driver"));
            put("jdbcUrl", Arrays.asList("jdbcUrl", "url"));
            put("database", Arrays.asList("database", "db"));
            put("port", Arrays.asList("port"));
            put("hostname", Arrays.asList("hostname", "host"));
        }
    };

    private void debugShow(String prefix, Object dsInfo) {
        if (Objects.isNull(dsInfo)) return;
        if (StringUtils.isBlank(prefix)) prefix = "";
        final String envShowDs = EnvUtil.getEnv(ENV_SHOW_DS_INFO_KEY);
        if (StringUtils.isNotBlank(envShowDs)) {
            String dsStr = dsInfo.toString();
            if (dsInfo instanceof Map) {
                Map<String, Object> dsMap = (Map) dsInfo;
                if ("true".equalsIgnoreCase(envShowDs))
                    dsStr = dsMap.entrySet().stream().map(x -> "     " + x.getKey() + " -> " + "xxx").collect(Collectors.joining("\n", "------------------------ begin\n", "\n------------------------ end"));
                else if ("all".equalsIgnoreCase(envShowDs))
                    dsStr = dsMap.entrySet().stream().map(x -> "     " + x.getKey() + " -> " + (Objects.nonNull(x.getValue()) ? x.getValue().toString() : "")).collect(Collectors.joining("\n", "------------------------ begin\n", "\n------------------------ end"));
            }
            log.info("【dolphin ds】 {}: {}", prefix, dsStr);
        }
    }

    private Config parse(Config pluginCfg) {
        Config outCfg = pluginCfg;
        debugShow("pluginCfg input", outCfg.root().render(ConfigRenderOptions.concise().setFormatted(true)));
        try {
            if (pluginCfg.hasPathOrNull(cfgDataSourcePath) && StringUtils.isNotBlank(pluginCfg.getString(cfgDataSourcePath))) {
                final String dataSourceName = pluginCfg.getString(cfgDataSourcePath);
                log.info("parsing data source id:【{}】", dataSourceName);
                final Map<String, Object> remoteDataSourceMap = remoteReq(dataSourceName);
                debugShow("[" + dataSourceName + "] remoteReq parse", remoteDataSourceMap);
                if (remoteDataSourceMap.containsKey("password")) {
                    remoteDataSourceMap.put("password", DesUtil.getInstance().decryptor(remoteDataSourceMap.getOrDefault("password", "").toString()));
                }
                log.info("parsing completed. data source:【{}】, size:【{}】", remoteDataSourceMap.isEmpty() ? "failed" : "success", remoteDataSourceMap.size());
                if (!remoteDataSourceMap.isEmpty()) {
                    for (Map.Entry<String, List<String>> mapEntry : dataSourceMapping.entrySet()) {
                        final String mapKey = mapEntry.getKey();
                        final List<String> mappingPaths = mapEntry.getValue();
                        Object remoteDataSourceNameValue = remoteDataSourceMap.get(mapKey);
                        for (String mappingPath : mappingPaths) {
                            outCfg = injectToCfg(outCfg, remoteDataSourceNameValue, mappingPath);
                        }
                    }
                    outCfg = injectToCfg(outCfg, remoteDataSourceMap, cfgOutDataSourcePath);
                    for (Iterator<Map.Entry<String, ConfigValue>> it = outCfg.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<String, ConfigValue> cfgEntry = it.next();
                        String k = cfgEntry.getKey();
                        k = parseVar(k, remoteDataSourceMap);
                        ConfigValue cfgV = cfgEntry.getValue();
                        if (cfgV != null) {
                            Object v = cfgV;
                            if (cfgV.valueType().equals(ConfigValueType.STRING)) {
                                v = parseVar(cfgV.unwrapped() == null ? "" : cfgV.unwrapped().toString(), remoteDataSourceMap);
                            } else if (cfgV.valueType().equals(ConfigValueType.LIST)) {
                                v = ((List) cfgV.unwrapped()).stream().map(x -> parseVar(x == null ? "" : x.toString(), remoteDataSourceMap)).collect(Collectors.toList());
                            }
                            outCfg = injectToCfg(outCfg, v, k);
                        }
                        outCfg = injectToCfg(outCfg, k, ".");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        debugShow("pluginCfg output", outCfg.root().render(ConfigRenderOptions.concise().setFormatted(true)));
        return outCfg;
    }

    private String parseVar(String content, Map<String, Object> kmMap) {
        if (StringUtils.isBlank(content) || MapUtils.isEmpty(kmMap)) return content;
        //我是：${username} -> 我是：zs
        String elRegex = "\\$\\{(.*?)}";
        Pattern pattern = Pattern.compile(elRegex);
        String output = content;
        Matcher matcher;
        HashMap<String, String> tmpMap = new HashMap<>();
        while ((matcher = pattern.matcher(output)).find()) {
            //替换匹配内容
            String elStr = matcher.group();
            String k = matcher.group(1);
            String defaultValue = "";
            String v = Optional.ofNullable(kmMap.get(k)).orElse(defaultValue).toString();
            if (!kmMap.containsKey(k)) {
                v = UUID.randomUUID().toString() + Instant.now().toEpochMilli() + RandomUtils.nextInt(1, 9999);
                tmpMap.put(elStr, v);
            }
            output = output.replace(elStr, v);
        }
        for (Map.Entry<String, String> tmpMapEntry : tmpMap.entrySet()) {
            output = output.replace(tmpMapEntry.getValue(), tmpMapEntry.getKey());
        }
        return output;
    }

    private Config injectToCfg(Config cfg, Object value, String path) {
        boolean isMatch = false;
        if (!cfg.hasPath(path)) {
            isMatch = true;
        } else if (StringUtils.isNotBlank(path) && Pattern.matches(".*" + pattern + ".*", path)) {
            isMatch = true;
        } else
            for (Iterator<Map.Entry<String, ConfigValue>> it = cfg.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, ConfigValue> entry = it.next();
                String oldK = entry.getKey();
                ConfigValue oldV = entry.getValue();
                if (path.equals(oldK) && oldV.unwrapped() != null && Pattern.matches(".*" + pattern + ".*", oldV.unwrapped().toString()))
                    isMatch = true;
            }
        if (isMatch) {
            return cfg.withValue(path, ConfigValueFactory.fromAnyRef(value));
        }
        return cfg;
    }

    private void setValue(String key, Object value) {
        if (value != null)
            parseDataSourceMap.put(key, StringUtils.strip(value.toString(), "\""));
        else parseDataSourceMap.put(key, value);
    }

    private Map<String, Object> remoteReq(String dataSourceName) {
        String envDsHostPort = EnvUtil.getEnv(ENV_DS_HOSTPORT_KEY);
        String envDsToken = EnvUtil.getEnv(ENV_DS_TOKEN_KEY);
        String envShowDs = EnvUtil.getEnv(ENV_SHOW_DS_INFO_KEY);
        try {
            if (envDsHostPort != null && envDsToken != null) {
                String url = URL_TEMPLATE.replace("HOST_PORT", envDsHostPort).replace("DS_NAME", dataSourceName);
                Map<String, String> headers = new HashMap<>();
                headers.put("Accept", "application/json");
                headers.put("token", envDsToken);
                HttpHelper.HttpR resp = HttpHelper.doHttp(url, "GET", headers, null);
                if (resp.isOK(200) && resp.getBody() != null) {
                    ObjectNode bodyNode = JsonUtils.parseObject(resp.getBody());
                    if (bodyNode != null && bodyNode.hasNonNull("success") && bodyNode.get("success").asBoolean() && bodyNode.hasNonNull("data")) {
                        JsonNode dsPageBodyNode = bodyNode.get("data");
                        if (dsPageBodyNode != null && dsPageBodyNode.hasNonNull("totalList")) {
                            JsonNode dsListNode = dsPageBodyNode.get("totalList");
                            for (JsonNode dsNode : dsListNode) {
                                for (Iterator<String> it = dsNode.fieldNames(); dataSourceName.equals(dsNode.get("name").asText()) && it.hasNext(); ) {
                                    String dsKey = it.next();
                                    setValue(dsKey, dsNode.get(dsKey));
                                    if (dsKey.equals("connectionParams")) {
                                        JsonNode paramNode = JsonUtils.parseObject(dsNode.get(dsKey).asText());
                                        for (Iterator<String> it2 = paramNode.fieldNames(); it2.hasNext(); ) {
                                            String paramKey = it2.next();
                                            if ("props".equals(paramKey)) {
                                                JsonNode propNode = paramNode.get(paramKey);
                                                for (Iterator<String> it3 = propNode.fieldNames(); it3.hasNext(); ) {
                                                    String propKey = it3.next();
                                                    setValue(propKey, propNode.get(propKey));
                                                }
                                            } else
                                                setValue(paramKey, paramNode.get(paramKey));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else
                log.warn("the environment variable does not contain key:{},{}", ENV_DS_HOSTPORT_KEY, ENV_DS_TOKEN_KEY);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return parseDataSourceMap;
    }


    @Override
    public List<? extends Config> convertCfgs(List<? extends Config> cfgs) {
        return CollectionUtils.isEmpty(cfgs) ? Collections.emptyList() : cfgs.stream()
                .map(this::parse)
                .collect(Collectors.toList());
    }

    @Override
    public String getPluginName() {
        return "dolphin_datasource";
    }
}
