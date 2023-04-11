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

package org.apache.seatunnel.translation.flink.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.transfrom.DataTypeInfo;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.serialization.FlinkRowConverter;
import org.apache.seatunnel.translation.flink.statistics.SinkStatistics;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class FlinkSinkWriter<InputT, CommT, WriterStateT> implements SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> {

    private final org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter;
    private final FlinkRowConverter rowSerialization;
    private long checkpointId;
    private final SinkStatistics sinkStatistics;

    FlinkSinkWriter(org.apache.seatunnel.api.sink.SinkWriter<SeaTunnelRow, CommT, WriterStateT> sinkWriter,
                    long checkpointId,
                    SinkStatistics sinkStatistics) {
//        System.err.println(this + " [LOG] -> init:" + Arrays.asList(sinkWriter,checkpointId,sinkStatistics));
        this.sinkWriter = sinkWriter;
        this.checkpointId = checkpointId;
        this.sinkStatistics = sinkStatistics;

        SeaTunnelSink<SeaTunnelRow, ?, ?, ?> sinkStatisticsPlugin = sinkStatistics.getPlugin();
        SeaTunnelDataType<SeaTunnelRow> dataType = sinkStatisticsPlugin.getConsumedType();
        Function<String, SeaTunnelDataType<?>> dataTypeFun = sinkStatisticsPlugin::getDynamicRowType;
        log.info("FlinkSinkWriterFlinkSinkWriterFlinkSinkWriterFlinkSinkWriter"+sinkStatisticsPlugin+" "+dataTypeFun);
        this.rowSerialization = new FlinkRowConverter(new DataTypeInfo(true, dataType, dataTypeFun));
    }

    @Override
    public void write(InputT element, org.apache.flink.api.connector.sink.SinkWriter.Context context) throws IOException {
//        System.err.println(this + " [LOG] -> write:" + context);
        if (element instanceof Row) {
            Row record = (Row) element;
            this.sinkStatistics.statistics(record);
            sinkWriter.write(rowSerialization.reconvert(record));
        } else {
            throw new InvalidClassException("only support Flink Row at now, the element Class is " + element.getClass());
        }
    }

    @Override
    public List<CommitWrapper<CommT>> prepareCommit(boolean flush) throws IOException {
//        System.err.println(this + " [LOG] -> prepareCommit:" + flush);
        Optional<CommT> commTOptional = sinkWriter.prepareCommit();
        return commTOptional.map(CommitWrapper::new).map(Collections::singletonList).orElse(Collections.emptyList());
    }

    @Override
    public List<FlinkWriterState<WriterStateT>> snapshotState() throws IOException {
//        System.err.println(this + " [LOG] -> snapshotState:" + null);
        List<FlinkWriterState<WriterStateT>> states = sinkWriter.snapshotState(this.checkpointId)
                .stream().map(state -> new FlinkWriterState<>(this.checkpointId, state)).collect(Collectors.toList());
        this.checkpointId++;
        return states;
    }

    @Override
    public void close() throws Exception {
//        System.err.println(this + " [LOG] -> close:" + null);
        sinkWriter.close();
        this.sinkStatistics.close();
    }
}
