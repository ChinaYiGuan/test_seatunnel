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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class JdbcSourceSplitEnumerator implements SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceSplitEnumerator.class);
    private final SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext;
    private final Map<Integer, Set<JdbcSourceSplit>> pendingSplits;
    private JdbcSourceOptions jdbcSourceOptions;
    private final Function<Integer, List<JdbcSourceSplit>> splitFunction;

    public JdbcSourceSplitEnumerator(Context<JdbcSourceSplit> enumeratorContext, JdbcSourceOptions jdbcSourceOptions, Function<Integer, List<JdbcSourceSplit>> splitFunction) {
        this.enumeratorContext = enumeratorContext;
        this.jdbcSourceOptions = jdbcSourceOptions;
        this.splitFunction = splitFunction;
        this.pendingSplits = new HashMap<>();
    }

    @Override
    public void open() {
        // No connection needs to be opened
    }

    @Override
    public void run() throws Exception {
        discoverySplits();
        assignPendingSplits();
    }


    private void discoverySplits() {
        LOG.info("Starting to calculate splits.");
        int numReaders = enumeratorContext.currentParallelism();
        List<JdbcSourceSplit> allSplit = splitFunction.apply(numReaders);
        for (JdbcSourceSplit split : allSplit) {
            int ownerReader = split.splitId % numReaders;
            pendingSplits.computeIfAbsent(ownerReader, r -> new HashSet<>())
                    .add(split);
        }
        LOG.debug("Assigned {} to {} readers.", allSplit, numReaders);
        LOG.info("Calculated splits successfully, the size of splits is {}.", allSplit.size());
    }


    private void assignPendingSplits() {
        // Check if there's any pending splits for given readers
        for (int pendingReader : enumeratorContext.registeredReaders()) {
            // Remove pending assignment for the reader
            final Set<JdbcSourceSplit> pendingAssignmentForReader =
                    pendingSplits.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Assign pending splits to reader
                LOG.info("Assigning splits to readers {}", pendingAssignmentForReader);
                enumeratorContext.assignSplit(pendingReader, new ArrayList<>(pendingAssignmentForReader));
            }
            enumeratorContext.signalNoMoreSplits(pendingReader);
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void addSplitsBack(List<JdbcSourceSplit> splits, int subtaskId) {
    }

    @Override
    public int currentUnassignedSplitSize() {
        return 0;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {

    }

    @Override
    public void registerReader(int subtaskId) {
        // nothing
    }

    @Override
    public JdbcSourceState snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
