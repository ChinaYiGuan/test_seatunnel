package org.apache.seatunnel.datasource.dolphin;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class DolphinDataSourceTest {

    public static void main(String[] args) {
        //dev
//        System.setProperty("dataSourceHost", "http://localhost:12345");
//        System.setProperty("dataSourceToken", "701761fd7842afa31d14f9ecec5fd5d0");
        //uat2
        System.setProperty("dataSourceHost", "http://10.192.147.2:12345");
        System.setProperty("dataSourceToken", "f94d3be770bad18b080def5953564a8d");
        //uat1
//        System.setProperty("dataSourceHost", "http://10.192.147.1:12345");
//        System.setProperty("dataSourceToken", "bf77ef162b00c9d03bc8849ee1d60407");

        System.setProperty("is_show_parseDs", "true");
        System.setProperty("is_show_parseDs", "all");
        DolphinDataSource dolphinDataSource = new DolphinDataSource();
        HashMap<String, Object> cfgMap = new HashMap<>();
        cfgMap.put("datasource", "uat_doris-147.1");
        Config config = ConfigFactory.parseMap(cfgMap);
        List<? extends Config> configs = dolphinDataSource.convertCfgs(Collections.singletonList(config));
        System.err.println(configs.get(0).root().render(ConfigRenderOptions.concise().setFormatted(true)));
    }
}
