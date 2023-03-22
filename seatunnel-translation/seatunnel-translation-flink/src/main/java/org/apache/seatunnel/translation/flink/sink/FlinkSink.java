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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.flink.serialization.CommitWrapperSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkSimpleVersionedSerializer;
import org.apache.seatunnel.translation.flink.serialization.FlinkWriterStateSerializer;
import org.apache.seatunnel.translation.flink.statistics.SinkStatistics;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FlinkSink<InputT, CommT, WriterStateT, GlobalCommT> implements Sink<InputT, CommitWrapper<CommT>,
        FlinkWriterState<WriterStateT>, GlobalCommT> {

    private final SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink;
    private final Config sinkCfg;
    private final SinkStatistics sinkStatistics;

    public FlinkSink(SeaTunnelSink<SeaTunnelRow, WriterStateT, CommT, GlobalCommT> sink, Config sinkCfg) {
//        System.err.println(this + " [LOG] -> init:" + Arrays.asList(sink,sinkCfg));
        this.sink = sink;
        this.sinkCfg = sinkCfg;
        this.sinkStatistics = new SinkStatistics(sinkCfg, sink);
    }

    @Override
    public SinkWriter<InputT, CommitWrapper<CommT>, FlinkWriterState<WriterStateT>> createWriter(org.apache.flink.api.connector.sink.Sink.InitContext context, List<FlinkWriterState<WriterStateT>> states) throws IOException {
//        System.err.println(this + " [LOG] -> createWriter:" + Arrays.asList(context,states));
        this.sinkStatistics.open();
        org.apache.seatunnel.api.sink.SinkWriter.Context stContext = new DefaultSinkWriterContext(context.getSubtaskId());
        if (states == null || states.isEmpty()) {
            return new FlinkSinkWriter<>(sink.createWriter(stContext), 1, sinkStatistics);
        } else {
            List<WriterStateT> restoredState = states.stream().map(FlinkWriterState::getState).collect(Collectors.toList());
            return new FlinkSinkWriter<>(sink.restoreWriter(stContext, restoredState), states.get(0).getCheckpointId(), sinkStatistics);
        }

    }

    @Override
    public Optional<Committer<CommitWrapper<CommT>>> createCommitter() throws IOException {
//        System.err.println(this + " [LOG] -> createCommitter:" + null);
        return sink.createCommitter().map(FlinkCommitter::new);
    }

    @Override
    public Optional<GlobalCommitter<CommitWrapper<CommT>, GlobalCommT>> createGlobalCommitter() throws IOException {
//        System.err.println(this + " [LOG] -> createGlobalCommitter:" + null);
        return sink.createAggregatedCommitter().map(FlinkGlobalCommitter::new);
    }

    @Override
    public Optional<SimpleVersionedSerializer<CommitWrapper<CommT>>> getCommittableSerializer() {
//        System.err.println(this + " [LOG] -> getCommittableSerializer:" + null);
        return sink.getCommitInfoSerializer().map(CommitWrapperSerializer::new);
    }

    @Override
    public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
//        System.err.println(this + " [LOG] -> getGlobalCommittableSerializer:" + null);
        return sink.getAggregatedCommitInfoSerializer().map(FlinkSimpleVersionedSerializer::new);
    }

    @Override
    public Optional<SimpleVersionedSerializer<FlinkWriterState<WriterStateT>>> getWriterStateSerializer() {
//        System.err.println(this + " [LOG] -> getWriterStateSerializer:" + null);
        return sink.getWriterStateSerializer().map(FlinkWriterStateSerializer::new);
    }
}
