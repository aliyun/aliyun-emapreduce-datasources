/**
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

package com.aliyun.openservices.tablestore.hadoop;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TableStoreRecordWriterV2 extends RecordWriter<Writable, BatchWriteWritable> {
    private static final Logger logger = LoggerFactory.getLogger(TableStoreRecordWriterV2.class);

    private TableStoreWriter ots;

    private AtomicLong rowCounter = new AtomicLong(0);
    private Deque<RowChange> waitingRows = new ArrayDeque<RowChange>();

    private int maxBatchCount;
    private boolean isIgnoreOnFailureEnabled;

    public TableStoreRecordWriterV2(TableStoreWriter ots, SinkConfig sinkConfig) {
        Preconditions.checkNotNull(ots, "ots client must be nonnull.");
        this.ots = ots;
        this.ots.setResultCallback(new TableStoreCallback<RowChange, RowWriteResult>() {
            @Override
            public void onCompleted(RowChange req, RowWriteResult res) {
                rowCounter.incrementAndGet();
            }

            @Override
            public void onFailed(RowChange req, Exception ex) {
                logger.error("Batch write failed because client write failed, row change: {}, " +
                        "exception: {}", req, ex);
                if (!isIgnoreOnFailureEnabled) {
                    waitingRows.addFirst(req);
                    throw new RuntimeException("Task for batch write failed", ex);
                }
            }
        });
        this.maxBatchCount = sinkConfig.getWriterMaxBatchCount();
        this.isIgnoreOnFailureEnabled = sinkConfig.isIgnoreOnFailureEnabled();
        logger.debug("Max batch size: {}", this.maxBatchCount);
        logger.debug("Whether ignore on failure is enabled: {}", this.isIgnoreOnFailureEnabled);
    }

    @Override
    public void write(Writable writable, BatchWriteWritable batchWriteWritable)
            throws IOException, InterruptedException {
        List<RowChange> rows = batchWriteWritable.getRowChanges();
        for (RowChange row : rows) {
            waitingRows.addLast(row);
        }
        while (waitingRows.size() >= maxBatchCount) {
            batchWrite();
        }
    }

    private void batchWrite() {
        // prepare rows
        List<RowChange> rows = new ArrayList<>();
        for (int i = 0; i < maxBatchCount && !waitingRows.isEmpty(); i++) {
            RowChange row = waitingRows.pollFirst();
            rows.add(row);
        }

        // add row change to client
        List<RowChange> dirtyRows = new ArrayList<>();
        try {
            ots.addRowChange(rows, dirtyRows);
        } catch (ClientException e) {
            // handle dirty data
            if (dirtyRows.size() > 0) {
                logger.error("Batch write failed because it contains dirty rows, count: {}, " +
                                "sample dirty row change: {}", dirtyRows.size(), dirtyRows.get(0));
                if (!isIgnoreOnFailureEnabled) {
                    for (RowChange dirtyRow : dirtyRows) {
                        waitingRows.addFirst(dirtyRow);
                    }
                    throw new RuntimeException("Task for Batch write failed");
                }
            }
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        while (!waitingRows.isEmpty()) {
            batchWrite();
        }
        ots.close();
        logger.info("This task wrote {} rows", rowCounter);
    }
}
