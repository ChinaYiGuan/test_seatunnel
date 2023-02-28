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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor;

import com.mysql.cj.jdbc.ClientPreparedStatement;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.dynamic.RowIdentifier;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.TabMeta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

@RequiredArgsConstructor
@Slf4j
public class InsertOrUpdateBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    private final StatementFactory existStmtFactory;
    @NonNull
    private final StatementFactory insertStmtFactory;
    @NonNull
    private final StatementFactory updateStmtFactory;
    @NonNull
    private final BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor;
    @NonNull
    private final BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor;
    @NonNull
    private final Function<String, TabMeta> tabMetaFun;
    @NonNull
    private final JdbcRowConverter rowConverter;
    private transient Map<String, PreparedStatement> existStatementMap;
    private transient Map<String, PreparedStatement> insertStatementMap;
    private transient Map<String, PreparedStatement> updateStatementMap;
    private transient Connection connection;
    private transient Boolean preExistFlag;
    private transient boolean submitted;

    public InsertOrUpdateBatchStatementExecutor(StatementFactory insertStmtFactory,
                                                StatementFactory updateStmtFactory,
                                                BiFunction<String, SeaTunnelRow, SeaTunnelRow> keyExtractor,
                                                BiFunction<String, SeaTunnelRow, SeaTunnelRow> valueExtractor,
                                                Function<String, TabMeta> tabMetaFun,
                                                JdbcRowConverter rowConverter) {
        this(null, insertStmtFactory, updateStmtFactory, keyExtractor, valueExtractor, tabMetaFun, rowConverter);
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
//        if (upsertMode()) {
//            existStatement = existStmtFactory.createStatement(connection);
//        }
//        insertStatement = insertStmtFactory.createStatement(connection);
//        updateStatement = updateStmtFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        final String identifier = Optional.ofNullable(record.getRowIdentifier()).map(RowIdentifier::getIdentifier).orElse(null);
        final SeaTunnelRowType valueRowType = tabMetaFun.apply(identifier).getSeaTunnelRowType();

        SeaTunnelRow newRecord = valueExtractor.apply(identifier, record);
        if (upsertMode()) {
            PreparedStatement existStatement = existStatementMap.computeIfAbsent(identifier, x -> {
                try {
                    PreparedStatement sts = existStmtFactory.createStatement(connection, identifier);
                    if (sts instanceof ClientPreparedStatement) {
                        String q = ((ClientPreparedStatement) sts).getPreparedSql();
                        log.info("existStatement identifier:{}, record size:{}, sql:{}", identifier, newRecord.getArity(), q);
                    }
                    return sts;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
        }
        PreparedStatement insertStatement = insertStatementMap.computeIfAbsent(identifier, x -> {
            try {
                PreparedStatement sts = insertStmtFactory.createStatement(connection, identifier);
                if (sts instanceof ClientPreparedStatement) {
                    String q = ((ClientPreparedStatement) sts).getPreparedSql();
                    log.info("insertStatement identifier:{}, record size:{}, sql:{}", identifier, newRecord.getArity(), q);
                }
                return sts;
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        PreparedStatement updateStatement = updateStatementMap.computeIfAbsent(identifier, x -> {
            try {
                PreparedStatement sts = updateStmtFactory.createStatement(connection, identifier);
                if (sts instanceof ClientPreparedStatement) {
                    String q = ((ClientPreparedStatement) sts).getPreparedSql();
                    log.info("updateStatement identifier:{}, record size:{}, sql:{}", identifier, newRecord.getArity(), q);
                }
                return sts;
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });

        boolean exist = existRow(newRecord, identifier);
        if (exist) {
            if (preExistFlag != null && !preExistFlag) {
                insertStatementMap.get(identifier).executeBatch();
                insertStatementMap.get(identifier).clearBatch();
            }
            rowConverter.toExternal(valueRowType, newRecord, updateStatement);
            updateStatementMap.get(identifier).addBatch();
        } else {
            if (preExistFlag != null && preExistFlag) {
                updateStatement.executeBatch();
                updateStatement.clearBatch();
            }
            rowConverter.toExternal(valueRowType, newRecord, insertStatement);
            insertStatement.addBatch();
        }

        preExistFlag = exist;
        submitted = false;
    }

    @Override
    public void executeBatch() throws SQLException {
        if (preExistFlag != null) {
            if (preExistFlag) {
                updateStatementMap.forEach((identifier, updateStatement) -> {
                    try {
                        updateStatement.executeBatch();
                        updateStatement.clearBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
//                updateStatement.executeBatch();
//                updateStatement.clearBatch();
            } else {
                insertStatementMap.forEach((identifier, insertStatement) -> {
                    try {
                        insertStatement.executeBatch();
                        insertStatement.clearBatch();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
//                insertStatement.executeBatch();
//                insertStatement.clearBatch();
            }
        }
        submitted = true;
    }

    @Override
    public void closeStatements() throws SQLException {
        if (!submitted) {
            executeBatch();
        }
        for (Map<String, PreparedStatement> statementMap : Arrays.asList(existStatementMap, insertStatementMap, updateStatementMap)) {
            if (statementMap != null) {
                statementMap.forEach((identifier, statement) -> {
                    try {
                        if (statement != null)
                            statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
            }
        }
//        for (PreparedStatement statement : Arrays.asList(existStatement, insertStatement, updateStatement)) {
//            if (statement != null) {
//                statement.close();
//            }
//        }
    }

    private boolean upsertMode() {
        return existStmtFactory != null;
    }

    private boolean existRow(SeaTunnelRow record, String identifier) throws SQLException {
        if (upsertMode()) {
            SeaTunnelRow keyRow = keyExtractor.apply(identifier, record);
            return exist(record, identifier);
        }
        switch (record.getRowKind()) {
            case INSERT:
                return false;
            case UPDATE_AFTER:
                return true;
            default:
                throw new JdbcConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                        "unsupported row kind: " + record.getRowKind());
        }
    }

    private boolean exist(SeaTunnelRow keyRow, String identifier) throws SQLException {
        TabMeta tabMeta = tabMetaFun.apply(identifier);
        if (tabMeta.isKeyTab()) {
            //Object[] fields
            PreparedStatement existStatement = existStatementMap.computeIfAbsent(identifier, x -> {
                try {
                    return existStmtFactory.createStatement(connection, identifier);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
            String[] pkNames = tabMeta.getColMetas()
                    .stream()
                    .filter(TabMeta.ColMeta::getIsKey)
                    .map(TabMeta.ColMeta::getName)
                    .toArray(String[]::new);
            SeaTunnelDataType<?>[] pkTypes = tabMeta.getColMetas().stream()
                    .filter(TabMeta.ColMeta::getIsKey)
                    .map(TabMeta.ColMeta::getType)
                    .toArray(SeaTunnelDataType[]::new);
            rowConverter.toExternal(new SeaTunnelRowType(pkNames, pkTypes), keyRow, existStatement);
            try (ResultSet resultSet = existStatement.executeQuery()) {
                return resultSet.next();
            }
        }
        return false;
    }
}