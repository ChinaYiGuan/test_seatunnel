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

package org.apache.seatunnel.core.starter.spark.command;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.config.ConfigBuilder;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.spark.execution.SparkExecution;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

/**
 * todo: do we need to move these class to a new module? since this may cause version conflict with the old Spark version.
 * This command is used to execute the Spark job by SeaTunnel new API.
 */
@Slf4j
public class SparkApiTaskExecuteCommand implements Command<SparkCommandArgs> {

    private final SparkCommandArgs sparkCommandArgs;

    public SparkApiTaskExecuteCommand(SparkCommandArgs sparkCommandArgs) {
        this.sparkCommandArgs = sparkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile = FileUtils.getConfigPath(sparkCommandArgs);
        checkConfigExist(configFile);
        Config config = new ConfigBuilder(configFile, sparkCommandArgs.getVariables()).getConfig();
        try {
            SparkExecution seaTunnelTaskExecution = new SparkExecution(config);
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            log.error("Run SeaTunnel on spark failed.", e);
            throw new CommandExecuteException(e.getMessage());
        }
    }

}
