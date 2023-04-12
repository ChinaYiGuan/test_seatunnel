/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used to build the {@link  Config} from file.
 */
@Slf4j
public class ConfigBuilder {

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final Path configFile;
    private final Config config;
    private final List<String> variables;

    public ConfigBuilder(Path configFile) {
        this(configFile, null);
    }

    public ConfigBuilder(Path configFile, List<String> variables) {
        this.configFile = configFile;
        this.variables = variables;
        this.config = load();
    }

    private static String parseEL(String input, Map<String, Object> mapMap) {
        String elRegex = "#\\{(.*?)}";
        Pattern pattern = Pattern.compile(elRegex);
        String output = input;
        Matcher matcher;
        try {
            while ((matcher = pattern.matcher(output)).find()) {
                //替换匹配内容
                String elStr = matcher.group();
                String k = matcher.group(1);
                String v = Optional.ofNullable(mapMap.get(k)).map(Object::toString).orElse(elStr);
                if (mapMap.containsKey(k)) {
                    log.info("cfg replace variables:【{}】 -> 【{}】", k, v);
                }
                output = matcher.replaceFirst(v);
            }
        } catch (Exception e) {
            log.error("cfg replace err:" + e.getMessage());
        }
        return output;
    }

    private Config load() {
        if (configFile == null) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        log.info("Loading config file: {}", configFile);
        String cfgStr = FileUtils.readFileToStr(configFile.toFile().toPath());

        if (CollectionUtils.isNotEmpty(variables)) {
            Map<String, Object> varMap = variables.stream()
                    .filter(StringUtils::isNotBlank)
                    .filter(x -> x.split("=").length == 2)
                    .map(x -> x.split("="))
                    .collect(HashMap::new, (m, x) -> m.put(x[0], x[1]), Map::putAll);
            cfgStr = parseEL(cfgStr, varMap);
        }

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        Config config = ConfigFactory
//                .parseFile(configFile.toFile())
                .parseString(cfgStr)
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        log.info("parsed config file: {}", config.root().render(options));
        return config;
    }

    public Config getConfig() {
        return config;
    }

}
