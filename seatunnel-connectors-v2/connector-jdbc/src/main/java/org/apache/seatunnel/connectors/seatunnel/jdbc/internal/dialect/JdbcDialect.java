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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;

import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */

public interface JdbcDialect extends Serializable {

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object to seatunnel internal object.
     *
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter();


    /**
     * get jdbc meta-information type to seatunnel data type mapper.
     *
     * @return a type mapper for the database
     */
    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

    /**
     * Quotes the identifier for table name or field name
     */
    default String quoteIdentifier(String identifier) {
        return identifier;
    }

    /**
     * Constructs the dialects insert statement for a single row. The returned string will be
     * used as a {@link java.sql.PreparedStatement}. Fields in the statement must be in the same
     * order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * INSERT INTO table_name (column_name [, ...]) VALUES (value [, ...])
     * }</pre>
     *
     * @return the dialects {@code INSERT INTO} statement.
     */
    default String getInsertIntoStatement(String tableName, String[] fieldNames) {
        String columns = Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fieldNames)
                .map(fieldName -> "?")
                .collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                quoteIdentifier(tableName), columns, placeholders);
    }

    /**
     * Constructs the dialects update statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * UPDATE table_name SET col = val [, ...] WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code UPDATE} statement.
     */
    default String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
        String setClause = Arrays.stream(fieldNames)
                .map(fieldName -> String.format("%s = ?", quoteIdentifier(fieldName)))
                .collect(Collectors.joining(", "));
        String conditionClause = Arrays.stream(conditionFields)
                .map(fieldName -> String.format("%s = ?", quoteIdentifier(fieldName)))
                .collect(Collectors.joining(" AND "));
        return String.format("UPDATE %s SET %s WHERE %s",
                quoteIdentifier(tableName), setClause, conditionClause);
    }

    /**
     * Constructs the dialects delete statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * DELETE FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code DELETE} statement.
     */
    default String getDeleteStatement(String tableName, String[] conditionFields) {
        String conditionClause = Arrays.stream(conditionFields)
                .map(fieldName -> format("%s = ?", quoteIdentifier(fieldName)))
                .collect(Collectors.joining(" AND "));
        return String.format("DELETE FROM %s WHERE %s",
                quoteIdentifier(tableName), conditionClause);
    }

    /**
     * Generates a query to determine if a row exists in the table. The returned string will be used
     * as a {@link java.sql.PreparedStatement}.
     *
     * <pre>{@code
     * SELECT 1 FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code QUERY} statement.
     */
    default String getRowExistsStatement(String tableName, String[] conditionFields) {
        String fieldExpressions = Arrays.stream(conditionFields)
                .map(field -> format("%s = ?", quoteIdentifier(field)))
                .collect(Collectors.joining(" AND "));
        return String.format("SELECT 1 FROM %s WHERE %s",
                quoteIdentifier(tableName), fieldExpressions);
    }

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}.
     * <p>
     * If supported, the returned string will be used as a {@link java.sql.PreparedStatement}.
     * Fields in the statement must be in the same order as the {@code fieldNames} parameter.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT ROW Exists} + {@code UPDATE}/{@code INSERT} which may have poor performance.
     *
     * @return the dialects {@code UPSERT} statement or {@link Optional#empty()}.
     */
    Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields);

    /**
     * Different dialects optimize their PreparedStatement
     *
     * @return The logic about optimize PreparedStatement
     */
    default PreparedStatement creatPreparedStatement(Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    default ResultSetMetaData getResultSetMetaData(Connection conn, JdbcSourceOptions jdbcSourceOptions) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(String.format("SELECT * FROM ( %s ) T1 WHERE 1=0", jdbcSourceOptions.getQuery()));
        return ps.getMetaData();
    }

    default List<String> listDatabase(Connection conn, JdbcSourceOptions jdbcSourceOptions) throws SQLException {
        String listDbsSql = "show databases";
        try (ResultSet rs = conn.createStatement().executeQuery(listDbsSql)) {
            List<Map<String, Object>> list = SimpleJdbcConnectionProvider.getList(rs);
            List<String> dbs = list.stream()
                    .flatMap(x -> x.entrySet().stream()
                            .map(Map.Entry::getValue)
                            .filter(Objects::nonNull)
                            .map(Object::toString)
                    ).distinct()
                    .collect(Collectors.toList());
            return dbs;
        }
    }

    default List<String> listTables(Connection conn, JdbcSourceOptions jdbcSourceOptions, String db) throws SQLException {
        String useDbSql = "use " + db;
        Statement statement = conn.createStatement();
        statement.execute(useDbSql);
        String listTabsSql = "show tables";
        try (ResultSet rs = statement.executeQuery(listTabsSql)) {
            List<Map<String, Object>> list = SimpleJdbcConnectionProvider.getList(rs);
            List<String> dbTabs = list.stream()
                    .flatMap(x -> x.entrySet().stream()
                            .map(Map.Entry::getValue)
                            .filter(Objects::nonNull)
                            .map(Object::toString)
                            .map(y -> y)
                    ).distinct()
                    .collect(Collectors.toList());
            return dbTabs;
        }
    }

}
