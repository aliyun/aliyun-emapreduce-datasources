/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.openservices.tablestore.hadoop;

import java.util.List;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreRecordWriter extends RecordWriter<Writable, BatchWriteWritable> {
    private static final Logger logger = LoggerFactory.getLogger(TableStoreRecordWriter.class);

    private SyncClientInterface ots;
    private String outputTable;
    private long rowCounter;
    private Deque<RowChange> waitingRows = new ArrayDeque<RowChange>();
    private static int BATCH_SIZE_MAX = 200;
    private static int BATCH_SIZE_INCR = 10;
    private static double BATCH_SIZE_BACKOFF = 0.8;
    private int batchSize = BATCH_SIZE_MAX;

    public TableStoreRecordWriter(SyncClientInterface ots, String outputTable) {
        Preconditions.checkNotNull(ots, "ots client must be nonnull.");
        Preconditions.checkNotNull(outputTable, "output table must be nonnull.");
        this.ots = ots;
        this.outputTable = outputTable;
        this.rowCounter = 0;
    }

    @Override public void write(Writable _, BatchWriteWritable batch) {
        List<RowChange> rows = batch.getRowChanges();
        for(RowChange row: rows) {
            waitingRows.addLast(row);
        }
        for(; waitingRows.size() >= batchSize;) {
            rowCounter += batchWrite();
        }
    }

    @Override public void close(TaskAttemptContext ctx) {
        for(;!waitingRows.isEmpty();) {
            rowCounter += batchWrite();
        }
        logger.info("this task wrote {} rows", rowCounter);
        ots.shutdown();
    }

    private int batchWrite() {
        while(true) {
            Deque<RowChange> rows = prepareRows();
            if (rows.size() < batchSize) {
                logger.info("small batch size: {}", rows.size());
            } else {
                logger.debug("batch size: {}", rows.size());
            }
            try {
                launch(rows);
                batchSize += BATCH_SIZE_INCR;
                if (batchSize > BATCH_SIZE_MAX) {
                    batchSize = BATCH_SIZE_MAX;
                }
                return rows.size();
            } catch(TableStoreException ex) {
                if (ex.getErrorCode() != "OTSParameterInvalid"
                    || batchSize == 1) {
                    throw ex;
                } else {
                    logger.info("Batch-size backs off. current batch-size: {}", rows.size());
                    batchSize = (int) (rows.size() * BATCH_SIZE_BACKOFF);
                    if (batchSize < 1) {
                        batchSize = 1;
                    }
                    while(!rows.isEmpty()) {
                        RowChange row = rows.pollLast();
                        waitingRows.addFirst(row);
                    }
                }
            }
        }
    }

    private Deque<RowChange> prepareRows() {
        Deque<RowChange> res = new ArrayDeque<RowChange>();
        Set<Integer> detectDupRows = new HashSet<Integer>();
        for(int rowCnt = 0; rowCnt < batchSize && !waitingRows.isEmpty(); ++rowCnt) {
            RowChange row = waitingRows.pollFirst();
            int hash = row.getTableName().hashCode() * 1327144901
                + row.getPrimaryKey().hashCode();
            if (detectDupRows.contains(hash)) {
                break;
            }
            res.addLast(row);
            detectDupRows.add(hash);
        }
        return res;
    }

    private void launch(Deque<RowChange> rows) {
        BatchWriteRowRequest req = new BatchWriteRowRequest();
        for(RowChange row: rows) {
            req.addRowChange(row);
        }
        BatchWriteRowResponse resp = ots.batchWriteRow(req);
        List<BatchWriteRowResponse.RowResult> failed = resp.getFailedRows();
        for(BatchWriteRowResponse.RowResult res: failed) {
            logger.error("fail to write to TableStore. table: {} error-code: {} error-message: {} request-id: {}",
                res.getTableName(),
                res.getError().getCode(),
                res.getError().getMessage(),
                resp.getRequestId());
        }
        if (!failed.isEmpty()) {
            Error err = failed.get(0).getError();
            throw new TableStoreException(
                err.getMessage(), null, err.getCode(), resp.getRequestId(), 0);
        }
    }
}
