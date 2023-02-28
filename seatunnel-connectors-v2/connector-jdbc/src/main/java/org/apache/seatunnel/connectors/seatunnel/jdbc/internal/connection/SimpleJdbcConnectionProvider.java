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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection;

import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple JDBC connection provider.
 */
public class SimpleJdbcConnectionProvider
        implements JdbcConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private static final long serialVersionUID = 1L;

    private final JdbcConnectionOptions jdbcOptions;

    private transient Driver loadedDriver;
    private transient Connection connection;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }


    public static List<Map<String, Object>> getList(ResultSet rs) {
        List<Map<String, Object>> targets = new ArrayList<>();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int colCnt = rsmd.getColumnCount();
            String[] columnNames = new String[colCnt];
            Object[] columnValues = new Object[colCnt];
            Class<?>[] columnTypes = new Class<?>[colCnt];
            for (int i = 0; i < colCnt; i++) {
                columnNames[i] = rsmd.getColumnLabel(i + 1);
            }
            while (rs.next()) {
                for (int i = 0; i < colCnt; i++) {
                    String columnName = columnNames[i];
                    Object columnValue = rs.getObject(columnName);
                    Class<?> columnType = null;
                    if (columnValue != null) {
                        columnType = columnValue.getClass();
                    }
                    columnValues[i] = columnValue;
                    columnTypes[i] = columnType;
                }
                Map<String, Object> target = new LinkedHashMap<>();
                for (int i = 0; i < colCnt; i++) {
                    String columnName = columnNames[i];
                    Object columnValue = columnValues[i];
                    target.put(columnName, columnValue);
                }
                targets.add(target);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return targets;
    }

    public static <T> List<T> getList(ResultSet rs, Class<T> clazz) {
        List<Map<String, Object>> ls = getList(rs);
        List<T> targets = new ArrayList<>();
        try {
            for (int i = 0; i < ls.size(); i++) {
                Map<String, Object> x = ls.get(i);
                T target = clazz.newInstance();
                for (Iterator<Map.Entry<String, Object>> it = x.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<String, Object> y = it.next();
                    String columnName = y.getKey();
                    Object columnValue = y.getValue();
                    Field field = clazz.getDeclaredField(columnName);
                    //Class<?> fieldType = field.getType();
                    field.setAccessible(true);
                    field.set(target, columnValue);
                }
                targets.add(target);
            }
        } catch (InstantiationException | IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        return targets;
    }

    public SeaTunnelRowType getTabMeta(String tableFullName) {
        JdbcDialect jdbcDialect = JdbcDialectLoader.load(jdbcOptions.getUrl());
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        try {
            Connection conn = getOrEstablishConnection();
            ResultSetMetaData resultSetMetaData = jdbcDialect.getResultSetMetaData(conn, tableFullName, true);
            ArrayList<String> fieldNames = new ArrayList<>();
            ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnName(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
            return new SeaTunnelRowType(fieldNames.toArray(new String[0]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    public SimpleJdbcConnectionProvider(@NonNull JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean isConnectionValid()
            throws SQLException {
        return connection != null
                && connection.isValid(jdbcOptions.getConnectionCheckTimeoutSeconds());
    }

    private static Driver loadDriver(String driverName)
            throws ClassNotFoundException {
        checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }

        // We could reach here for reasons:
        // * Class loader hell of DriverManager(see JDK-8146872).
        // * driver is not installed as a service provider.
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new JdbcConnectorException(JdbcConnectorErrorCode.CREATE_DRIVER_FAILED, "Fail to create driver of class " + driverName, ex);
        }
    }

    private Driver getLoadedDriver()
            throws SQLException, ClassNotFoundException {
        if (loadedDriver == null) {
            loadedDriver = loadDriver(jdbcOptions.getDriverName());
        }
        return loadedDriver;
    }

    @Override
    public Connection getOrEstablishConnection()
            throws SQLException, ClassNotFoundException {
        if (connection != null) {
            return connection;
        }
        Driver driver = getLoadedDriver();
        Properties info = new Properties();
        if (jdbcOptions.getUsername().isPresent()) {
            info.setProperty("user", jdbcOptions.getUsername().get());
        }
        if (jdbcOptions.getPassword().isPresent()) {
            info.setProperty("password", jdbcOptions.getPassword().get());
        }
        connection = driver.connect(jdbcOptions.getUrl(), info);
        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DRIVER, "No suitable driver found for " + jdbcOptions.getUrl());
        }

        connection.setAutoCommit(jdbcOptions.isAutoCommit());

        return connection;
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }

    @Override
    public Connection reestablishConnection()
            throws SQLException, ClassNotFoundException {
        closeConnection();
        return getOrEstablishConnection();
    }
}
