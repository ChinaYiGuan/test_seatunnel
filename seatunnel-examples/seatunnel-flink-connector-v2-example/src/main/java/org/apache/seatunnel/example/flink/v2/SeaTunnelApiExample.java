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

package org.apache.seatunnel.example.flink.v2;

import org.apache.seatunnel.core.starter.Seatunnel;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.command.FlinkCommandBuilder;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;

public class SeaTunnelApiExample {

    public static void main(String[] args) throws FileNotFoundException, URISyntaxException, CommandException {
//        System.setProperty("dataSourceHost", "http://10.192.147.2:12345");
//        System.setProperty("dataSourceToken", "f94d3be770bad18b080def5953564a8d");
//        System.setProperty("dataSourceHost", "http://10.192.147.1:12345");
//        System.setProperty("dataSourceToken", "bf77ef162b00c9d03bc8849ee1d60407");
//        System.setProperty("is_show_parseDs", "all");

//        System.setProperty("dataSourceHost", "http://localhost:12345");
//        System.setProperty("dataSourceToken", "701761fd7842afa31d14f9ecec5fd5d0");
        System.setProperty("dataSourceHost", "http://10.192.112.26:12345");
        System.setProperty("dataSourceToken", "134f5bf1afe3a721aaa0d3a6ef8d6000");
        System.setProperty("is_show_parseDs", "all");

        String cfgFile = "/examples/example02.conf";
        String configurePath = args.length > 0 ? args[0] : cfgFile;
        String configFile = getTestConfigFile(configurePath);
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
//        flinkCommandArgs.setVariables(null);
        flinkCommandArgs.setVariables(Arrays.asList("table_suffix=_20230413","start_day=2013-04-13","end_day=2023-05-15"));
        Command<FlinkCommandArgs> flinkCommand =
                new FlinkCommandBuilder().buildCommand(flinkCommandArgs);
        Seatunnel.run(flinkCommand);
    }

    public static String getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelApiExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
