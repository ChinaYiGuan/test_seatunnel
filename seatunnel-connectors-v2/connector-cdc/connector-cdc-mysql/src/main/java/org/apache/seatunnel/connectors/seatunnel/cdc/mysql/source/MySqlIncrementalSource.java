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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import com.google.auto.service.AutoService;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.common.DynamicRowType;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.MySqlCatalog;

import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
public class MySqlIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> implements DynamicRowType<T> {

    public String getPluginName() {
        return "MySQL-CDC";
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.serverId(config.get(JdbcSourceOptions.SERVER_ID));
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, DebeziumDeserializationSchema<T>> createDebeziumDeserializationSchemaMap(ReadonlyConfig config) {
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        String baseUrl = config.get(JdbcCatalogOptions.BASE_URL);
        MySqlCatalog mySqlCatalog = new MySqlCatalog("mysql", "", jdbcSourceConfig.getUsername(), jdbcSourceConfig.getPassword(), baseUrl);
        List<String> dbs = mySqlCatalog.listDatabases().stream()
                .filter(findDbName -> jdbcSourceConfig.getDatabaseList().stream().anyMatch(dbNamePt -> Pattern.matches(dbNamePt, findDbName)))
                .collect(Collectors.toList());
        List<String> tabs = dbs.stream()
                .flatMap(dbName ->
                        mySqlCatalog.listTables(dbName).stream()
                                .map(findTabName -> dbName + "." + findTabName)
                                .filter(findFullTabName -> jdbcSourceConfig.getTableList().stream().anyMatch(tabNamePt -> Pattern.matches(tabNamePt, findFullTabName)))
                ).filter(dbTabName -> dbTabName.contains(".") && dbTabName.split("\\.").length == 2)
                .collect(Collectors.toList());

        Map<String, DebeziumDeserializationSchema<T>> collect = tabs.stream()
                .map(dbTab -> {
                    final CatalogTable table = mySqlCatalog.getTable(TablePath.of(dbTab));
                    final SeaTunnelRowType physicalRowType = table.getTableSchema().toPhysicalRowDataType();
                    final String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
                    return Pair.of(dbTab.split("\\.")[1],
                            (DebeziumDeserializationSchema<T>) SeaTunnelRowDebeziumDeserializeSchema.builder()
                                    .setPhysicalRowType(physicalRowType)
                                    .setResultTypeInfo(physicalRowType)
                                    .setServerTimeZone(ZoneId.of(zoneId))
                                    .build()
                    );
                }).collect(LinkedHashMap::new, (m, x) -> m.put(x.getLeft(), x.getRight()), Map::putAll);

        return collect;
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new MySqlDialect((MySqlSourceConfigFactory) configFactory);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new BinlogOffsetFactory((MySqlSourceConfigFactory) configFactory, (JdbcDataSourceDialect) dataSourceDialect);
    }

    @Override
    public SeaTunnelDataType<T> getDynamicRowType(String identifier) {
        if (MapUtils.isNotEmpty(deserializationSchemaMap) && deserializationSchemaMap.containsKey(identifier)) {
            return deserializationSchemaMap.get(identifier).getProducedType();
        }
        return null;
    }
}
