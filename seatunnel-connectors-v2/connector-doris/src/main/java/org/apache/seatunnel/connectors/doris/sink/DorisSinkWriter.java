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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.dynamic.RowIdentifier;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.doris.client.DorisSinkManager;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;
import org.apache.seatunnel.connectors.doris.domain.Record;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.util.DelimiterParserUtil;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.common.util.ElParseUtil;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.text.TextSerializationSchema;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class DorisSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private ReadonlyConfig readonlyConfig;

    private final DorisSinkManager manager;

    private final SinkConfig sinkConfig;
    private final Map<String, SeaTunnelRowType> seaTunnelRowTypeMap;


    public DorisSinkWriter(Config pluginConfig,
                           Map<String, SeaTunnelRowType> seaTunnelRowTypeMap) {
        this.sinkConfig = SinkConfig.loadConfig(pluginConfig);
        this.seaTunnelRowTypeMap = seaTunnelRowTypeMap;
        this.manager = new DorisSinkManager(sinkConfig);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        log.info("writewritewritewritewrite  ->>>>>>"+element);
        String identifier = Optional.ofNullable(element.getRowIdentifier()).map(RowIdentifier::getIdentifier).orElse(null);
        String fullTableName = ElParseUtil.parseTableFullName(sinkConfig.getTable(), sinkConfig.getTableEl(), identifier);
        SeaTunnelRowType dataType = seaTunnelRowTypeMap.get(fullTableName);
        if (Objects.isNull(dataType))
            log.error("get seaTunnelRowTypeMap empty. identifier:{}, fullTableName:{} ,seaTunnelRowTypeMap:{}", identifier, fullTableName, seaTunnelRowTypeMap);
        SerializationSchema serializer = createSerializer(sinkConfig, dataType);
        String record = new String(serializer.serialize(element));
        manager.write(new Record(identifier, fullTableName, record));
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        manager.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            throw new DorisConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED,
                    "Close doris manager failed.", e);
        }
    }

    public static SerializationSchema createSerializer(SinkConfig sinkConfig, SeaTunnelRowType seaTunnelRowType) {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            String columnSeparator = DelimiterParserUtil.parse(sinkConfig.getColumnSeparator(), "\t");
            return TextSerializationSchema.builder()
                    .seaTunnelRowType(seaTunnelRowType)
                    .delimiter(columnSeparator)
                    .build();
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return new JsonSerializationSchema(seaTunnelRowType);
        }
        throw new DorisConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                "Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
