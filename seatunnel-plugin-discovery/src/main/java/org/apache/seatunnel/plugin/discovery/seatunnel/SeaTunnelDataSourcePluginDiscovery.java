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

package org.apache.seatunnel.plugin.discovery.seatunnel;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.datasource.BaseDataSource;
import org.apache.seatunnel.common.utils.EnvUtil;
import org.apache.seatunnel.plugin.discovery.AbstractPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.List;
import java.util.function.BiConsumer;

@Slf4j
public class SeaTunnelDataSourcePluginDiscovery extends AbstractPluginDiscovery<BaseDataSource> {

    private static final String pluginSubDir = "seatunnel";

    public SeaTunnelDataSourcePluginDiscovery() {
        super(pluginSubDir);
    }

    public SeaTunnelDataSourcePluginDiscovery(BiConsumer<ClassLoader, URL> addURLToClassLoader) {
        super(pluginSubDir, addURLToClassLoader);
    }

    @Override
    protected Class<BaseDataSource> getPluginBaseClass() {
        return BaseDataSource.class;
    }

    public BaseDataSource discovery(String dataSourcePluginName) {
        PluginIdentifier pluginIdentifier = PluginIdentifier.of("seatunnel", "transform", StringUtils.isNotBlank(dataSourcePluginName) ? dataSourcePluginName : EnvUtil.getEnv("datasource_plugin", "dolphin_datasource"));
        try {
            return createPluginInstance(pluginIdentifier);
        } catch (Exception e) {
            log.warn("datasource plugin " + pluginIdentifier + " not found. use default datasource plugin !");
            return defaultDiscovery();
        }
    }

    private BaseDataSource defaultDiscovery() {
        return new BaseDataSource() {
            @Override
            public List<? extends Config> convertCfgs(List<? extends Config> cfgs) {
                return defCvtCfgs(cfgs);
            }

            @Override
            public String getPluginName() {
                return "default_datasource";
            }
        };
    }

}
