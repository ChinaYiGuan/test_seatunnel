package org.apache.seatunnel.connectors.seatunnel.common.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElParseUtil {

    public static String parse(String input, Map<String, Object> mapMap) {
        String elRegex = "\\$\\{(.*?)}";
        Pattern pattern = Pattern.compile(elRegex);
        String output = input;
        Matcher matcher;
        while ((matcher = pattern.matcher(output)).find()) {
            //替换匹配内容
            String elStr = matcher.group();
            String k = matcher.group(1);
            String defaultValue = "";
            output = output.replace(elStr, Optional.ofNullable(mapMap.get(k)).orElse(defaultValue).toString());
        }
        return output;
    }

    public static String parseTableFullName(String tab, String tabEl, String identifier) {
        return StringUtils.isNotBlank(tab) ? tab : StringUtils.isNotBlank(tabEl) ? ElParseUtil.parse(tabEl, Collections.singletonMap("identifier", identifier)) : tabEl;
    }

    public static void main(String[] args) {
        String input = "你好${abc} 开心：${cdfd}";
        HashMap<String, Object> mapMap = new HashMap<>();
        mapMap.put("abc", 1234);
        String output = parse(input, mapMap);
        System.out.println(output);
    }

}
