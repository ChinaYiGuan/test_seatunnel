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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.dynamic.RowIdentifier;
import org.apache.seatunnel.connectors.seatunnel.common.util.ElParseUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.*;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.TabMeta;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class JdbcOutputFormatBuilder {
    @NonNull
    private final JdbcDialect dialect;
    @NonNull
    private final JdbcConnectionProvider connectionProvider;
    @NonNull
    private final JdbcSinkOptions jdbcSinkOptions;
    @NonNull
    private final Map<String, SeaTunnelRowType> seaTunnelRowTypeMap;


    /**
     * @param pkNames             example -> ["id","name"] or ["db1.tab1:id,name","db1.tab2:id,name,fk"]
     * @param seaTunnelRowTypeMap
     * @return
     */
    private Function<String, TabMeta> initTabMetaFun(List<String> pkNames, Map<String, SeaTunnelRowType> seaTunnelRowTypeMap, Function<String, String> fullTableNameFun) {
        final List<String> tmpKeys = CollectionUtils.isEmpty(pkNames) ? Collections.emptyList() : pkNames;
        return identifier -> {
            SeaTunnelRowType seaTunnelRowType = seaTunnelRowTypeMap.get(fullTableNameFun.apply(identifier));
            if (tmpKeys.stream().allMatch(y -> y.split(":").length == 2)) {
                List<String> pkNamesTmp = tmpKeys.stream()
                        .map(pkName -> pkName.split(":"))
                        .filter(pkName -> pkName[0].trim().equals(identifier))
                        .map(pkName -> pkName[1])
                        .filter(StringUtils::isNotBlank)
                        .map(String::trim)
                        .collect(Collectors.toList());
                tmpKeys.clear();
                tmpKeys.addAll(pkNamesTmp);
            }
            String[] fieldNames = seaTunnelRowType.getFieldNames();

            List<Integer> pkTypeIndexs = tmpKeys.stream()
                    .map(pkName -> {
                        int pkIndex = Arrays.asList(fieldNames).indexOf(pkName);
                        if (pkIndex == -1)
                            throw new RuntimeException("pkName配置没有对应上. key:" + pkName);
                        return pkIndex;
                    })
                    .collect(Collectors.toList());
            if (pkTypeIndexs.stream().anyMatch(y -> -1 == y)) {
                throw new RuntimeException("pkName配置没有对应上:" + tmpKeys);
            }
            List<TabMeta.ColMeta> colMetas = new ArrayList<>();
            for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
                String fieldName = seaTunnelRowType.getFieldName(i);
                SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
                boolean isKey = tmpKeys.contains(fieldName);
                colMetas.add(new TabMeta.ColMeta(i, fieldName, fieldType, isKey));
            }

            return new TabMeta(identifier, seaTunnelRowType, colMetas);
        };
    }

    private BiFunction<String, SeaTunnelRow, SeaTunnelRow> initKeyExtractor(Function<String, TabMeta> tabMetaFun) {
        return (identifier, record) -> {
            TabMeta tabMeta = tabMetaFun.apply(identifier);
            if (tabMeta.isKeyTab()) {
                List<TabMeta.ColMeta> keyColMetas = tabMeta.getColMetas().stream().filter(x -> x.getIsKey()).collect(Collectors.toList());
                Object[] fields = tabMeta.getColMetas().stream().filter(x -> x.getIsKey())
                        .map(x -> record.getField(x.getIndex()))
                        .toArray(x -> new Object[x]);
                SeaTunnelRow keyRow = new SeaTunnelRow(fields);
                keyRow.setRowIdentifier(record.getRowIdentifier());
                keyRow.setRowKind(record.getRowKind());
                return keyRow;
            }
            return null;
        };
    }

    private BiFunction<String, SeaTunnelRow, SeaTunnelRow> initValueExtractor(Function<String, TabMeta> tabMetaFun) {
        return (identifier, record) -> record;
    }

    private Function<String, String> initFullTableNameFun(String tab, String tabEl) {
        return identifier -> ElParseUtil.parseTableFullName(tab, tabEl, identifier);
    }

    public JdbcOutputFormat build() {
        final String table = jdbcSinkOptions.getTable();
        final String simpleSQL = jdbcSinkOptions.getSimpleSQL();
        final String tableEl = jdbcSinkOptions.getTableEl();
        final List<String> primaryKeys = jdbcSinkOptions.getPrimaryKeys();
        Function<String, String> fullTableNameFun = initFullTableNameFun(table, tableEl);
        Function<String, TabMeta> tabMetaFun = initTabMetaFun(primaryKeys, seaTunnelRowTypeMap, fullTableNameFun);
        BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor = initKeyExtractor(tabMetaFun);
        BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor = initValueExtractor(tabMetaFun);
        JdbcOutputFormat.StatementExecutorFactory statementExecutorFactory;

        if (StringUtils.isNotBlank(table) || StringUtils.isNotBlank(tableEl)) {
            statementExecutorFactory = () -> createSimpleBufferedExecutor(dialect, null, fullTableNameFun, tabMetaFun);
            if (CollectionUtils.isNotEmpty(primaryKeys)) {
                statementExecutorFactory = () -> createUpsertBufferedExecutor(
                        dialect, fullTableNameFun, tabMetaFun, keyExtractor, valueExtractor,
                        jdbcSinkOptions.isSupportUpsertByQueryPrimaryKeyExist()
                );
            }
        } else if (StringUtils.isNotBlank(simpleSQL)) {
            statementExecutorFactory = () -> createSimpleBufferedExecutor(dialect, simpleSQL, fullTableNameFun, tabMetaFun);
        } else {
            throw new RuntimeException("sql cfg err.");
        }
        return new JdbcOutputFormat(connectionProvider,
                jdbcSinkOptions.getJdbcConnectionOptions(), statementExecutorFactory);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(JdbcDialect dialect,
                                                                                         String insertSql,
                                                                                         Function<String, String> fullTableNameFun,
                                                                                         Function<String, TabMeta> tabMetaFun) {
        JdbcBatchStatementExecutor<SeaTunnelRow> simpleRowExecutor = createSimpleExecutor(dialect.getRowConverter(), tabMetaFun,
                (identifier, tabMeta) -> Optional.ofNullable(insertSql).orElseGet(() -> dialect.getInsertIntoStatement(fullTableNameFun.apply(identifier), selectFields(tabMeta)))
        );
        return new BufferedBatchStatementExecutor(simpleRowExecutor, Function.identity());
    }


    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertBufferedExecutor(JdbcDialect dialect,
                                                                                         Function<String, String> fullTableNameFun,
                                                                                         Function<String, TabMeta> tabMetaFun,
                                                                                         BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor,
                                                                                         BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor,
                                                                                         boolean supportUpsertByQueryPrimaryKeyExist) {

        JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor = createDeleteExecutor(dialect, fullTableNameFun, tabMetaFun);
        JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor = createUpsertExecutor(dialect, fullTableNameFun, keyExtractor,
                valueExtractor, tabMetaFun, supportUpsertByQueryPrimaryKeyExist);
        return new BufferReducedBatchStatementExecutor(
                upsertExecutor, deleteExecutor, keyExtractor, valueExtractor);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertExecutor(JdbcDialect dialect,
                                                                                 Function<String, String> fullTableNameFun,
                                                                                 BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor,
                                                                                 BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor,
                                                                                 Function<String, TabMeta> tabMetaFun,
                                                                                 boolean supportUpsertByQueryPrimaryKeyExist) {

        return dialect.getUpsertStatement("dbTab", new String[]{"f0,f1,f2"}, new String[]{"f0"})
                .map(testUpsertSql -> createSimpleExecutor(dialect.getRowConverter(), tabMetaFun,
                                (identifier, tabMeta) -> dialect.getUpsertStatement(fullTableNameFun.apply(identifier), selectFields(tabMeta), selectKeys(tabMeta)).get()
                        )
                )
                .orElseGet(() -> {
                    if (supportUpsertByQueryPrimaryKeyExist) {
                        return createInsertOrUpdateByQueryExecutor(dialect.getRowConverter(), keyExtractor, valueExtractor, tabMetaFun,
                                (identifier, tabMeta) -> dialect.getRowExistsStatement(fullTableNameFun.apply(identifier), selectKeys(tabMeta)),
                                (identifier, tabMeta) -> dialect.getInsertIntoStatement(fullTableNameFun.apply(identifier), selectFields(tabMeta)),
                                (identifier, tabMeta) -> dialect.getUpdateStatement(fullTableNameFun.apply(identifier), selectFields(tabMeta), selectKeys(tabMeta))
                        );
                    }
                    return createInsertOrUpdateExecutor(dialect.getRowConverter(), keyExtractor, valueExtractor, tabMetaFun,
                            (identifier, tabMeta) -> dialect.getInsertIntoStatement(fullTableNameFun.apply(identifier), selectFields(tabMeta)),
                            (identifier, tabMeta) -> dialect.getUpdateStatement(fullTableNameFun.apply(identifier), selectFields(tabMeta), selectKeys(tabMeta))
                    );
                });
    }

    private static String[] selectKeys(TabMeta tabMeta) {
        if (tabMeta != null && CollectionUtils.isNotEmpty(tabMeta.getColMetas())) {
            return tabMeta.getColMetas().stream()
                    .filter(TabMeta.ColMeta::getIsKey)
                    .map(TabMeta.ColMeta::getName)
                    .toArray(String[]::new);
        }
        return new String[0];
    }

    private static String[] selectFields(TabMeta tabMeta) {
        if (tabMeta != null && CollectionUtils.isNotEmpty(tabMeta.getColMetas())) {
            return tabMeta.getColMetas().stream()
                    .map(TabMeta.ColMeta::getName)
                    .toArray(String[]::new);
        }
        return new String[0];
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateExecutor(JdbcRowConverter rowConverter,
                                                                                         BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor,
                                                                                         BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor,
                                                                                         Function<String, TabMeta> tabMetaFun,
                                                                                         BiFunction<String, TabMeta, String> statementInsertSqlFun,
                                                                                         BiFunction<String, TabMeta, String> statementUpdateSqlFun) {

        return new InsertOrUpdateBatchStatementExecutor(
                (connection, identifier) -> connection.prepareStatement(statementInsertSqlFun.apply(identifier, tabMetaFun.apply(identifier))),
                (connection, identifier) -> connection.prepareStatement(statementUpdateSqlFun.apply(identifier, tabMetaFun.apply(identifier))),
                keyExtractor,
                valueExtractor,
                tabMetaFun,
                rowConverter);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateByQueryExecutor(JdbcRowConverter rowConverter,
                                                                                                BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor,
                                                                                                BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor,
                                                                                                Function<String, TabMeta> tabMetaFun,
                                                                                                BiFunction<String, TabMeta, String> statementExistSqlFun,
                                                                                                BiFunction<String, TabMeta, String> statementInsertSqlFun,
                                                                                                BiFunction<String, TabMeta, String> statementUpdateSqlFun) {
        return new InsertOrUpdateBatchStatementExecutor(
                (connection, identifier) -> connection.prepareStatement(statementExistSqlFun.apply(identifier, tabMetaFun.apply(identifier))),
                (connection, identifier) -> connection.prepareStatement(statementInsertSqlFun.apply(identifier, tabMetaFun.apply(identifier))),
                (connection, identifier) -> connection.prepareStatement(statementUpdateSqlFun.apply(identifier, tabMetaFun.apply(identifier))),
                keyExtractor,
                valueExtractor,
                tabMetaFun,
                rowConverter);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createDeleteExecutor(JdbcDialect dialect,
                                                                                 Function<String, String> fullTableNameFun,
                                                                                 Function<String, TabMeta> tabMetaFun) {
        return createSimpleExecutor(dialect.getRowConverter(), tabMetaFun, (identifier, tabMeta) -> dialect.getDeleteStatement(fullTableNameFun.apply(identifier), selectKeys(tabMeta)));
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(JdbcRowConverter rowConverter,
                                                                                 Function<String, TabMeta> tabMetaFun,
                                                                                 BiFunction<String, TabMeta, String> statementSqlFun
    ) {
        return new SimpleBatchStatementExecutor(
                (connection, identifier) -> {
                    TabMeta tabMeta = tabMetaFun.apply(identifier);
                    String prepareStatementSql = statementSqlFun.apply(identifier, tabMeta);
                    return connection.prepareStatement(prepareStatementSql);
                },
                tabMetaFun,
                rowConverter);
    }

    private static Function<SeaTunnelRow, SeaTunnelRow> createKeyExtractor(Function<String, Pair<String, Pair<List<String>, List<Integer>>>> pkMappingNameTyIndFun) {
        return row -> {
            String identifier = Optional.ofNullable(row.getRowIdentifier()).map(RowIdentifier::getIdentifier).orElse(null);
            Pair<String, Pair<List<String>, List<Integer>>> pkMappingNameTyIndPair = pkMappingNameTyIndFun.apply(identifier);
            Pair<List<String>, List<Integer>> pkIdNameTyIndList = pkMappingNameTyIndPair.getRight();
            List<Integer> typeIndexs = pkIdNameTyIndList.getRight();
            Object[] fields = new Object[typeIndexs.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = row.getField(typeIndexs.get(i));
            }
            SeaTunnelRow newRow = new SeaTunnelRow(fields);
            newRow.setTableId(row.getTableId());
            newRow.setRowKind(row.getRowKind());
            return newRow;
        };
    }
}
