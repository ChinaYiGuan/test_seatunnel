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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;

import java.util.*;

import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isLowWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/**
 * The {@link RecordEmitter} implementation for {@link IncrementalSourceReader}.
 *
 * <p>The {@link RecordEmitter} buffers the snapshot records of split and call the stream reader to
 * emit records rather than emit the records directly.
 */
@Slf4j
public class IncrementalSourceRecordEmitter<T>
        implements RecordEmitter<SourceRecords, T, SourceSplitStateBase> {

    protected final Map<String, DebeziumDeserializationSchema<T>> deserializationSchemaMap;
    protected final OutputCollector<T> outputCollector;

    protected final OffsetFactory offsetFactory;

    public IncrementalSourceRecordEmitter(
            Map<String, DebeziumDeserializationSchema<T>> deserializationSchemaMap,
            OffsetFactory offsetFactory) {
        this.deserializationSchemaMap = deserializationSchemaMap;
        this.outputCollector = new OutputCollector<>();
        this.offsetFactory = offsetFactory;
    }

    @Override
    public void emitRecord(
            SourceRecords sourceRecords, Collector<T> collector, SourceSplitStateBase splitState)
            throws Exception {
        final Iterator<SourceRecord> elementIterator = sourceRecords.iterator();
        while (elementIterator.hasNext()) {
            processElement(elementIterator.next(), collector, splitState);
        }
    }

    protected void processElement(
            SourceRecord element, Collector<T> output, SourceSplitStateBase splitState)
            throws Exception {
        if (isWatermarkEvent(element)) {
            Offset watermark = getWatermark(element);
            if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
                splitState.asSnapshotSplitState().setHighWatermark(watermark);
            }
        } else if (isSchemaChangeEvent(element) && splitState.isIncrementalSplitState()) {
            //TODO Currently not supported Schema Change
        } else if (isDataChangeRecord(element)) {
            if (splitState.isIncrementalSplitState()) {
                Offset position = getOffsetPosition(element);
                splitState.asIncrementalSplitState().setStartupOffset(position);
            }
            emitElement(element, output);
        } else {
            // unknown element
            log.info("Meet unknown element {}, just skip.", element);
        }
    }

    private Offset getWatermark(SourceRecord watermarkEvent) {
        return getOffsetPosition(watermarkEvent.sourceOffset());
    }

    public Offset getOffsetPosition(SourceRecord dataRecord) {
        return getOffsetPosition(dataRecord.sourceOffset());
    }

    public Offset getOffsetPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return offsetFactory.specific(offsetStrMap);
    }

    protected void emitElement(SourceRecord element, Collector<T> output) throws Exception {
        outputCollector.output = output;

        if (Objects.nonNull(element) &&
                StringUtils.isNotBlank(element.topic()) &&
                element.topic().split("\\.").length >= 3 &&
                deserializationSchemaMap.containsKey(StringUtils.substringAfter(element.topic(),"."))
        ) {
            String dbTab = StringUtils.substringAfter(element.topic(),".");
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema = deserializationSchemaMap.get(dbTab);
            if (Objects.nonNull(debeziumDeserializationSchema)) {
                debeziumDeserializationSchema.deserialize(element, outputCollector);
            }
        } else
            deserializationSchemaMap.entrySet().stream().findFirst().get().getValue().deserialize(element, outputCollector);
    }

    private static class OutputCollector<T> implements Collector<T> {
        private Collector<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }
}
