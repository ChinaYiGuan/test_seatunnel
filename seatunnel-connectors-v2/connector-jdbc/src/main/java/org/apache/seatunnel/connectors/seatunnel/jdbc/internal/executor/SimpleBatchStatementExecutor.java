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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.dynamic.RowIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.TabMeta;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
@Slf4j
public class SimpleBatchStatementExecutor implements JdbcBatchStatementExecutor<SeaTunnelRow> {
    @NonNull
    private final StatementFactory statementFactory;
    @NonNull //table,dataType
    private final Function<String, TabMeta> tabMetaFun;
    @NonNull
    private final JdbcRowConverter converter;

    // table,statement
    private transient Map<String, PreparedStatement> statementMap = new HashMap<>(2);

    private transient Connection connection;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.connection = connection;
//        statement = statementFactory.createStatement(connection);
    }

    @Override
    public void addToBatch(SeaTunnelRow record) throws SQLException {
        String identifier = Optional.ofNullable(record.getRowIdentifier()).map(RowIdentifier::getIdentifier).orElse(null);
        SeaTunnelRowType rowType = tabMetaFun.apply(identifier).getSeaTunnelRowType();
        PreparedStatement statement = statementMap.computeIfAbsent(identifier, x -> {
            try {
                PreparedStatement sts = statementFactory.createStatement(connection, identifier);
                if (sts instanceof ClientPreparedStatement) {
                    String q = ((ClientPreparedStatement) sts).getPreparedSql();
                    log.info("statement identifier:{}, record size:{}, sql:{}", identifier, record.getArity(), q);
                }
                return sts;
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
        converter.toExternal(rowType, record, statement);
        statement.addBatch();
    }

    @Override
    public void executeBatch() throws SQLException {
        statementMap.forEach((identifier, statement) -> {
            try {
                if (!statement.isClosed()) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void closeStatements() throws SQLException {
        statementMap.forEach((identifier, statement) -> {
            try {
                if (!statement.isClosed()) {
                    statement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }
}
