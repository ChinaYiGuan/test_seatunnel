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

package org.apache.seatunnel.connectors.seatunnel.hive.config;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.utils.DbHelper;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HiveConfig {
    public static final Option<String> TABLE_NAME = Options.key("table_name")
            .stringType()
            .noDefaultValue()
            .withDescription("Hive table name");
    public static final Option<String> METASTORE_URI = Options.key("metastore_uri")
            .stringType()
            .noDefaultValue()
            .withDescription("Hive metastore uri");
    public static final Option<String> JDBC_URL = Options.key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("Hive jdbc url");
    public static final Option<String> JDBC_USERNAME = Options.key("username")
            .stringType()
            .defaultValue("")
            .withDescription("Hive jdbc username");
    public static final Option<String> JDBC_PASSWORD = Options.key("password")
            .stringType()
            .defaultValue("")
            .withDescription("Hive jdbc password");

    public static final Option<String> JDBC_DRIVER = Options.key("driver")
            .stringType()
            .defaultValue("")
            .withDescription("Hive jdbc driverClassName");

    public static final String TEXT_INPUT_FORMAT_CLASSNAME = "org.apache.hadoop.mapred.TextInputFormat";
    public static final String TEXT_OUTPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    public static final String PARQUET_INPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String PARQUET_OUTPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
    public static final String ORC_INPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    public static final String ORC_OUTPUT_FORMAT_CLASSNAME = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

    public static Pair<String[], Table> getTableInfo(Config config) {
        String table = config.getString(TABLE_NAME.key()).trim();
        String[] splits = table.split("\\.");
        if (splits.length != 2) {
            throw new RuntimeException("Please config " + TABLE_NAME + " as db.table format");
        }
        Table tableInformation;
        if (config.hasPath(HiveConfig.METASTORE_URI.key()) && StringUtils.isNotBlank(config.getString(HiveConfig.METASTORE_URI.key()))) {
            tableInformation = getTableInfoByMetaStore(config, splits);
        } else {
            tableInformation = getTableInfoByJdbc(config, splits);
        }

        System.err.println(JSON.toJSONString(tableInformation, JSONWriter.Feature.PrettyFormat));
        return Pair.of(splits, tableInformation);
    }

    private static Table getTableInfoByMetaStore(Config config, String[] dbTabArray) {
        HiveMetaStoreProxy hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(config);
        Table tableInformation = hiveMetaStoreProxy.getTable(dbTabArray[0], dbTabArray[1]);
        hiveMetaStoreProxy.close();
        return tableInformation;
    }

    private static Table getTableInfoByJdbc(Config config, String[] dbTabArray) {
        String url = config.getString(JDBC_URL.key());
        String username = config.hasPath(JDBC_USERNAME.key()) ? config.getString(JDBC_USERNAME.key()) : null;
        String password = config.hasPath(JDBC_PASSWORD.key()) ? config.getString(JDBC_PASSWORD.key()) : null;
        String driver = config.hasPath(JDBC_DRIVER.key()) ? config.getString(JDBC_DRIVER.key()) : null;
        DbHelper helper = new DbHelper(DbHelper.ConnInfo.builder()
                .url(url)
                .username(username)
                .password(password)
                .driver(driver)
                .props(Collections.singletonMap("sqlLevel", "info"))
                .build());
        List<Map<String, Object>> mapList = helper.query(String.format("show create table %s,%s", dbTabArray[0], dbTabArray[1]), null);
        System.out.println(mapList);
        return null;
    }
}
