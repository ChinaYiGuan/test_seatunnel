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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split.JdbcNumericBetweenParametersProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceCfgMeta;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
public class JdbcSource implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState> {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private JdbcSourceOptions jdbcSourceOptions;
    private Map<JdbcSourceCfgMeta, SeaTunnelRowType> typeInfoMap;
    private JdbcDialect jdbcDialect;
    private JdbcInputFormat inputFormat;
    private Map<JdbcSourceCfgMeta, PartitionParameter> partitionParameterMap;
    private JdbcConnectionProvider jdbcConnectionProvider;

    private List<JdbcSourceSplit> allSplit;

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        jdbcSourceOptions = new JdbcSourceOptions(pluginConfig);
        jdbcConnectionProvider = new SimpleJdbcConnectionProvider(jdbcSourceOptions.getJdbcConnectionOptions());
        jdbcDialect = JdbcDialectLoader.load(jdbcSourceOptions.getJdbcConnectionOptions().getUrl());
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            typeInfoMap = initTableFieldMap(connection);
            partitionParameterMap = initPartitionParameterAndExtendSqlMap(jdbcConnectionProvider.getOrEstablishConnection());
        } catch (Exception e) {
            e.printStackTrace();
            throw new PrepareFailException("jdbc", PluginType.SOURCE, e.toString());
        }

        inputFormat = new JdbcInputFormat(
                jdbcConnectionProvider,
                jdbcDialect,
                typeInfoMap,
                jdbcSourceOptions.getFetchSize(),
                jdbcSourceOptions.getJdbcConnectionOptions().isAutoCommit()
        );
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return typeInfoMap.entrySet().stream().findFirst().get().getValue();
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new JdbcSourceReader(inputFormat, readerContext);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext, jdbcSourceOptions, this::splitFunction);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext, JdbcSourceState checkpointState) throws Exception {
        return new JdbcSourceSplitEnumerator(enumeratorContext, jdbcSourceOptions, this::splitFunction);
    }

    private long queryTotalCount(Connection connection, String sqlTemplate, String partitionCol, Object min, Object max) {
        long total = 0;
        String sql = String.format("SELECT COUNT(*) FROM ( %s ) T1 WHERE ( %s >= %s AND %s <= %s ) OR ( %s IS NULL )", sqlTemplate, partitionCol,
                min instanceof Long ? min : String.format("'%s'", min.toString()),
                partitionCol,
                max instanceof Long ? max : String.format("'%s'", max.toString()),
                partitionCol
        );
        try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
            if (rs.next()) {
                total = Long.parseLong(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        LOG.info("queryTotalCount sql:{} -> total:{}", sql, total);
        return total;
    }

    private Object[][] buildPageBetween(Connection connection, int pageTotal, String sqlTemplate, String partitionCol, Object min, Object max) {
        List<Object[]> pages = new ArrayList<>();
        try {
            long totalCount = queryTotalCount(connection, sqlTemplate, partitionCol, min, max);
            int pageSize = (int) Math.ceil((double) totalCount / pageTotal);
            Object start = min;
            for (int i = 0; i < pageTotal; i++) {
                String conditoin = "<";
                if (i == pageTotal - 1) {
                    conditoin = "<=";
                }
                String sql = String.format("SELECT MAX(%s) FROM ( SELECT %s FROM ( %s ) T1 WHERE %s >= %s AND %s %s %s ORDER BY %s LIMIT %s ) T2 ",
                        partitionCol,
                        partitionCol,
                        sqlTemplate,
                        partitionCol,
                        start instanceof Long ? start : String.format("'%s'", start.toString()),
                        partitionCol,
                        conditoin,
                        max instanceof Long ? max : String.format("'%s'", max.toString()),
                        partitionCol,
                        pageSize
                );
                try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
                    if (rs.next()) {
                        Object end = rs.getObject(1);
                        LOG.info("buildPageBetween sql:{} -> start:{}. end:{}", sql, start, end);
                        if (end == null) continue;
                        pages.add(new Object[]{start, end});
                        start = end;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return pages.toArray(new Object[pages.size()][2]);
    }


    private List<JdbcSourceSplit> splitFunction(int parallelism) {
        if (allSplit == null) {
            allSplit = new ArrayList<>();
            Connection connection;
            try {
                connection = jdbcConnectionProvider.getOrEstablishConnection();
            } catch (SQLException | ClassNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            for (Iterator<Map.Entry<JdbcSourceCfgMeta, SeaTunnelRowType>> it = typeInfoMap.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<JdbcSourceCfgMeta, SeaTunnelRowType> typeInfoEntry = it.next();
                JdbcSourceCfgMeta sourceCfgMeta = typeInfoEntry.getKey();
                if (partitionParameterMap.containsKey(sourceCfgMeta)) {
                    PartitionParameter partitionParameter = partitionParameterMap.get(sourceCfgMeta);
                    int partitionNumber = partitionParameter.getPartitionNumber() != null ? partitionParameter.getPartitionNumber() : parallelism;
                    Object[][] parameterValues;
                    if (partitionParameter.getMin() instanceof Long && partitionParameter.getMax() instanceof Long) {
                        JdbcNumericBetweenParametersProvider JdbcNumericBetweenParametersProvider = new JdbcNumericBetweenParametersProvider(
                                Long.parseLong(partitionParameter.getMin().toString()),
                                Long.parseLong(partitionParameter.getMax().toString())).ofBatchNum(partitionNumber);
                        parameterValues = JdbcNumericBetweenParametersProvider.getParameterValues();
                    } else {
                        String sqlTemplate = sourceCfgMeta.getQuery();
                        String partitionCol = partitionParameter.getPartitionColumnName();
                        Object min = partitionParameter.getMin();
                        Object max = partitionParameter.getMax();
                        parameterValues = buildPageBetween(connection, partitionNumber, sqlTemplate, partitionCol, min, max);
                    }
                    for (int i = 0; i < parameterValues.length; i++) {
                        allSplit.add(new JdbcSourceSplit(parameterValues[i], i, sourceCfgMeta, partitionParameter.getPartitionColumnName(), i == parameterValues.length - 1));
                    }
                } else {
                    allSplit.add(new JdbcSourceSplit(null, 0, sourceCfgMeta, jdbcSourceOptions.getPartitionColumn().orElse(null), false));
                }
            }
        }
        return allSplit;
    }

    private List<String> dbTabsPattern(Connection conn) {
        try {
            List<String> filterDbs = jdbcDialect.listDatabase(conn)
                    .stream()
                    .filter(findDbName -> jdbcSourceOptions.getTables()
                            .stream()
                            .map(fullTableName -> fullTableName.split("\\.")[0])
                            .anyMatch(dbNamePt -> Pattern.matches(dbNamePt, findDbName))
                    )
                    .collect(Collectors.toList());

            return filterDbs.stream()
                    .flatMap(filterDbName ->
                            {
                                try {
                                    return jdbcDialect.listTables(conn, filterDbName)
                                            .stream()
                                            .map(findTabName -> filterDbName + "." + findTabName)
                                            .filter(findFullTabName -> jdbcSourceOptions.getTables().stream()
                                                    .anyMatch(tabNamePt -> Pattern.matches(tabNamePt, findFullTabName))
                                            );
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException(e);
                                }
                            }
                    ).filter(dbTabName -> dbTabName.contains(".") && dbTabName.split("\\.").length == 2)
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Map<JdbcSourceCfgMeta, SeaTunnelRowType> initTableFieldMap(Connection conn) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        List<JdbcSourceCfgMeta> tableCfgMetas;
        if (StringUtils.isNotBlank(jdbcSourceOptions.getQuery())) {
            tableCfgMetas = Arrays.asList(new JdbcSourceCfgMeta(null, null, jdbcSourceOptions.getQuery()));
        } else {
            List<String> filterFullTabs = dbTabsPattern(conn);
            tableCfgMetas = filterFullTabs.stream()
                    .map(x -> new JdbcSourceCfgMeta(StringUtils.substringBefore(x, "."), StringUtils.substringAfterLast(x, "."), String.format("SELECT * FROM %s", x)))
                    .collect(Collectors.toList());
        }

        Map<JdbcSourceCfgMeta, SeaTunnelRowType> seaTunnelRowTypeMap = new HashMap<>();
        for (JdbcSourceCfgMeta tableCfgMeta : tableCfgMetas) {
            String query = tableCfgMeta.getQuery();
            ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
            ArrayList<String> fieldNames = new ArrayList<>();
            try {
                String tmpOldQuery = jdbcSourceOptions.getQuery();
                jdbcSourceOptions.setQuery(query);
                ResultSetMetaData resultSetMetaData = jdbcDialect.getResultSetMetaData(conn, jdbcSourceOptions.getQuery(), false);
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    fieldNames.add(resultSetMetaData.getColumnName(i));
                    seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
                }
                jdbcSourceOptions.setQuery(tmpOldQuery);
            } catch (Exception e) {
                LOG.warn("get row type info exception", e);
            }
            seaTunnelRowTypeMap.put(tableCfgMeta, new SeaTunnelRowType(fieldNames.toArray(new String[0]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0])));
        }
        return seaTunnelRowTypeMap;
    }

    private PartitionParameter initPartitionParameter(String columnName, Connection connection, SeaTunnelDataType<?> partitionColumnType, JdbcSourceOptions jdbcSourceOpt) throws SQLException {
        Object max = Long.MAX_VALUE;
        Object min = Long.MIN_VALUE;
        String query = jdbcSourceOpt.getQuery();
        if (jdbcSourceOpt.getPartitionLowerBound().isPresent() && jdbcSourceOpt.getPartitionUpperBound().isPresent()) {
            max = jdbcSourceOpt.getPartitionUpperBound().get();
            min = jdbcSourceOpt.getPartitionLowerBound().get();
        } else {
            final String maxMinQuery = String.format("SELECT MAX(%s),MIN(%s) FROM (%s) TT", columnName, columnName, query);
            try (ResultSet rs = connection.createStatement().executeQuery(maxMinQuery)) {
                if (rs.next()) {
                    max = jdbcSourceOpt.getPartitionUpperBound().isPresent() ? jdbcSourceOpt.getPartitionUpperBound().get() :
                            rs.getObject(1);
                    min = jdbcSourceOpt.getPartitionLowerBound().isPresent() ? jdbcSourceOpt.getPartitionLowerBound().get() :
                            rs.getObject(2);
                }
            }
            LOG.info("jdbc source init query max min partition sql:{} -> min:{}. max:{}", maxMinQuery, min, max);
        }
        if (Objects.isNull(max)) max = 0;
        if (Objects.isNull(min)) min = 0;
        if (isNumericType(partitionColumnType)) {
            long maxVal = Long.parseLong(max.toString());
            long minVal = Long.parseLong(min.toString());
            max = maxVal;
            min = minVal;
        }
        PartitionParameter parameter = new PartitionParameter();
        parameter.setPartitionNumber(jdbcSourceOpt.getPartitionNumber().orElse(null));
        parameter.setPartitionColumnName(columnName);
        parameter.setMin(min);
        parameter.setMax(max);
        return parameter;
    }

    private Map<JdbcSourceCfgMeta, PartitionParameter> initPartitionParameterAndExtendSqlMap(Connection connection) throws SQLException {
        Map<JdbcSourceCfgMeta, PartitionParameter> partitionParameterMap = new HashMap<>();
        for (Iterator<Map.Entry<JdbcSourceCfgMeta, SeaTunnelRowType>> it = typeInfoMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<JdbcSourceCfgMeta, SeaTunnelRowType> typeInfoEntry = it.next();
            JdbcSourceCfgMeta sourceCfgMeta = typeInfoEntry.getKey();
            SeaTunnelRowType seaTunnelRowType = typeInfoEntry.getValue();
            String partitionColumn;
            if (jdbcSourceOptions.getPartitionColumn().isPresent()) {
                partitionColumn = jdbcSourceOptions.getPartitionColumn().get();
            } else if (jdbcSourceOptions.isAutoPartition()) {
                partitionColumn = seaTunnelRowType.getFieldNames()[0];
            } else {
                LOG.info("The partition_column parameter is not configured, and the source parallelism is set to 1");
                break;
            }
            if (!ArrayUtils.contains(seaTunnelRowType.getFieldNames(), partitionColumn)) {
                LOG.error("This partition column is not included, partitionColumn:{}", partitionColumn);
                throw new RuntimeException("his partition column is not included, partitionColumn:" + partitionColumn);
            }
            Map<String, SeaTunnelDataType<?>> fieldTypes = new HashMap<>();
            for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                fieldTypes.put(seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i));
            }
            SeaTunnelDataType<?> partitionColumnType = fieldTypes.get(partitionColumn);
            String tmpOldQuery = jdbcSourceOptions.getQuery();
            jdbcSourceOptions.setQuery(sourceCfgMeta.getQuery());
            PartitionParameter partitionParameter = initPartitionParameter(partitionColumn, connection, partitionColumnType, jdbcSourceOptions);
            jdbcSourceOptions.setQuery(tmpOldQuery);
            partitionParameterMap.put(sourceCfgMeta, partitionParameter);
        }
        return partitionParameterMap;
    }

    private boolean isNumericType(SeaTunnelDataType<?> type) {
        return type.equals(BasicType.INT_TYPE) || type.equals(BasicType.LONG_TYPE) || type.equals(BasicType.FLOAT_TYPE) || type.equals(BasicType.DOUBLE_TYPE);
    }

    public SeaTunnelDataType<SeaTunnelRow> getDynamicRowType(String identifier) {
        SeaTunnelDataType<SeaTunnelRow> value = typeInfoMap.entrySet().stream().filter(x -> x.getKey().getTable().equals(identifier)).findFirst().get().getValue();
        return value;
    }

    @Override
    public boolean isMultiple() {
        return typeInfoMap.size() > 1;
    }
}
