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

import java.util.ArrayDeque;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.conf.Configuration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreRecordReader extends RecordReader<PrimaryKeyWritable, RowWritable> {
    private static final Logger logger = LoggerFactory.getLogger(TableStoreRecordReader.class);
    
    private SyncClient ots;
    private RangeRowQueryCriteria scan;
    private PrimaryKey currentKey;
    private Row currentValue;
    private Iterator<Row> results;
    private long rowCounter;
    
    @Override public void close() {
        if (ots != null) {
            ots.shutdown();
            ots = null;
        }
        scan = null;
        currentKey = null;
        currentValue = null;
        results = null;
        rowCounter = 0;
    }

    @Override public PrimaryKeyWritable getCurrentKey() {
        return new PrimaryKeyWritable(currentKey);
    }

    @Override public RowWritable getCurrentValue() {
        return new RowWritable(currentValue);
    }

    @Override public float getProgress() {
        return 0;
    }

    @Override public boolean nextKeyValue() {
        if (!results.hasNext()) {
            logger.info("total rows: {}", rowCounter);
            return false;
        }
        currentValue = results.next();
        currentKey = currentValue.getPrimaryKey();
        ++rowCounter;
        if (rowCounter % 1000 == 0) {
            logger.info("deal with rows: {}", rowCounter);
        }
        return true;
    }

    @Override public void initialize(InputSplit split, TaskAttemptContext ctx) {
        initialize(split, ctx.getConfiguration());
    }

    /**
     * for internal usage only
     */
    public void initialize(InputSplit split, Configuration cfg) {
        close();

        Credential cred;
        {
            String in = cfg.get(TableStore.CREDENTIAL);
            Preconditions.checkNotNull(in, "Must set \"TABLESTORE_CREDENTIAL\"");
            cred = Credential.deserialize(in);
        }
        Endpoint ep;
        {
            String in = cfg.get(TableStore.ENDPOINT);
            Preconditions.checkNotNull(in, "Must set \"TABLESTORE_ENDPOINT\"");
            ep = Endpoint.deserialize(in);
        }
        if (cred.securityToken == null) {
            ots = new SyncClient(
                ep.endpoint,
                cred.accessKeyId,
                cred.accessKeySecret,
                ep.instance);
        } else {
            ots = new SyncClient(
                ep.endpoint,
                cred.accessKeyId,
                cred.accessKeySecret,
                ep.instance,
                cred.securityToken);
        }

        TableStoreInputSplit tsSplit = (TableStoreInputSplit) split;
        scan = tsSplit.getRangeRowQueryCriteria();
        logger.info("table: {} columns-to-get: {} start: {} end: {}",
            scan.getTableName(),
            scan.getColumnsToGet(),
            scan.getInclusiveStartPrimaryKey().toString(),
            scan.getExclusiveEndPrimaryKey().toString());
        results = ots.createRangeIterator(new RangeIteratorParameter(scan));
    }
}
