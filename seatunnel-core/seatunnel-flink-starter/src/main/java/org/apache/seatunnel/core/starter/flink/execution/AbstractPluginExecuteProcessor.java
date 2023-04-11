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

package org.apache.seatunnel.core.starter.flink.execution;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.datasource.BaseDataSource;
import org.apache.seatunnel.core.starter.flink.config.FlinkCommon;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelDataSourcePluginDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.seatunnel.apis.base.plugin.Plugin.SOURCE_TABLE_NAME;

@Slf4j
public abstract class AbstractPluginExecuteProcessor<T> implements PluginExecuteProcessor {

    protected FlinkEnvironment flinkEnvironment;
    protected final List<? extends Config> pluginConfigs;
    protected JobContext jobContext;
    protected final List<T> plugins;
    protected static final String ENGINE_TYPE = "seatunnel";
    protected static final String PLUGIN_NAME = "plugin_name";

    protected final BiConsumer<ClassLoader, URL> addUrlToClassloader = FlinkCommon.ADD_URL_TO_CLASSLOADER;

    protected AbstractPluginExecuteProcessor(List<URL> jarPaths, List<? extends Config> pluginConfigs, JobContext jobContext) {
        this.pluginConfigs = tsf(pluginConfigs);
        this.jobContext = jobContext;
        this.plugins = initializePlugins(jarPaths, this.pluginConfigs);
    }

    private List<? extends Config> tsf(List<? extends Config> inputPluginConfigs) {
        SeaTunnelDataSourcePluginDiscovery seaTunnelDataSourcePluginDiscovery = new SeaTunnelDataSourcePluginDiscovery(FlinkCommon.ADD_URL_TO_CLASSLOADER);
        BaseDataSource baseDataSource = seaTunnelDataSourcePluginDiscovery.discovery(null);
        log.info("Discovered using BaseDataSource conversion plugin: :{}", baseDataSource.getPluginName());
        return baseDataSource.convertCfgs(inputPluginConfigs);
    }

    @Override
    public void setFlinkEnvironment(FlinkEnvironment flinkEnvironment) {
        this.flinkEnvironment = flinkEnvironment;
    }

    protected abstract List<T> initializePlugins(List<URL> jarPaths, List<? extends Config> pluginConfigs);

    protected void registerResultTable(Config pluginConfig, DataStream<Row> dataStream) {
        flinkEnvironment.registerResultTable(pluginConfig, dataStream);
    }

    protected Optional<DataStream<Row>> fromSourceTable(Config pluginConfig) {
        if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            Table table = tableEnvironment.scan(pluginConfig.getString(SOURCE_TABLE_NAME));
            return Optional.ofNullable(TableUtil.tableToDataStream(tableEnvironment, table, true));
        }
        return Optional.empty();
    }
}
