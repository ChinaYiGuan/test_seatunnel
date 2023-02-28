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

package org.apache.seatunnel.connectors.doris.sink;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.doris.domain.rest.SchemaResp;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.util.DorisTypeConvertUtil;
import org.apache.seatunnel.connectors.doris.util.rest.DorisRestUtil;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.common.util.ElParseUtil;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.doris.config.SinkConfig.*;

@AutoService(SeaTunnelSink.class)
@Slf4j
public class DorisSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private DorisRestUtil restUtil;
    private String db;
    private String tab;
    private String tabEl;

    private final Map<String, SeaTunnelRowType> tsfTypes = new LinkedHashMap<>();

    @Override
    public String getPluginName() {
        return "Doris";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, NODE_URLS.key(), DATABASE.key(), USERNAME.key(), PASSWORD.key());
        CheckResult resultTab = CheckConfigUtil.checkAtLeastOneExists(pluginConfig, TABLE.key(), TABLE_EL.key());
        if (!result.isSuccess() || !resultTab.isSuccess()) {
            throw new DorisConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        this.db = pluginConfig.getString(DATABASE.key());
        this.tab = pluginConfig.hasPath(TABLE.key()) ? pluginConfig.getString(TABLE.key()) : null;
        this.tabEl = pluginConfig.hasPath(TABLE_EL.key()) ? pluginConfig.getString(TABLE_EL.key()) : null;

        String user = pluginConfig.getString(USERNAME.key());
        String pass = pluginConfig.getString(PASSWORD.key());
        String feHostPort = pluginConfig.getList(NODE_URLS.key()).get(0).unwrapped().toString();
        this.restUtil = new DorisRestUtil(
                feHostPort.split(":")[0]
                , Integer.parseInt(feHostPort.split(":")[1])
                , user
                , pass
        );
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        tsfTypes.put(tab, seaTunnelRowType);
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new DorisSinkWriter(pluginConfig, tsfTypes);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getDynamicRowType(String identifier) {
        String tableFullName = ElParseUtil.parseTableFullName(tab, tabEl, identifier);
        return tsfTypes.computeIfAbsent(tableFullName, x1 -> {
            SchemaResp schemaResp = restUtil.querySchema(tableFullName);
            if (schemaResp != null && schemaResp.getProperties() != null) {
                List<Pair<String, ? extends SeaTunnelDataType<?>>> nameTypePairList = schemaResp.getProperties()
                        .stream()
                        .map(x -> Pair.of(x.getName(), DorisTypeConvertUtil.convertFromColumn(x.getType()))).collect(Collectors.toList());
                String[] names = nameTypePairList.stream().map(Pair::getLeft).toArray(String[]::new);
                SeaTunnelDataType<?>[] seaTunnelDataTypes = nameTypePairList.stream().map(Pair::getRight).toArray(SeaTunnelDataType[]::new);
                return new SeaTunnelRowType(names, seaTunnelDataTypes);
            } else {
                log.warn("get doris table schema err. tab:{}", tab);
            }
            return null;
        });
    }

}