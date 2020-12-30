/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.openservices.tablestore.hadoop;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.ecosystem.FilterPushdownConfig;
import com.alicloud.openservices.tablestore.ecosystem.TablestoreSplit;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class TableStoreRecordReader extends RecordReader<PrimaryKeyWritable, RowWritable> {
    private static final Logger logger = LoggerFactory.getLogger(TableStoreRecordReader.class);

    private static SyncClient ots;
    private PrimaryKey currentKey;
    private Row currentValue;
    private Iterator<Row> results;
    private long rowCounter;

    @Override
    public void close() {
        currentKey = null;
        currentValue = null;
        results = null;
        rowCounter = 0;
    }

    @Override
    public PrimaryKeyWritable getCurrentKey() {
        return new PrimaryKeyWritable(currentKey);
    }

    @Override
    public RowWritable getCurrentValue() {
        return new RowWritable(currentValue);
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public boolean nextKeyValue() {
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

    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx) {
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
        FilterPushdownConfigSerialize filterPushdownConfigSerialize;
        {
            String in = cfg.get(TableStore.FILTER_PUSHDOWN_CONFIG);
            if( in!=null && !in.isEmpty()){
                filterPushdownConfigSerialize = FilterPushdownConfigSerialize.deserialize(in);
            }else {
                filterPushdownConfigSerialize = new
                        FilterPushdownConfigSerialize(false,false);
            }

        }
        if (cred.securityToken == null) {
            if (ots == null) {
                synchronized (TableStoreRecordReader.class) {
                    if (ots == null) {
                        ots = new SyncClient(
                            ep.endpoint, cred.accessKeyId, cred.accessKeySecret, ep.instance);
                    }
                }
            }
        } else {
            if (ots == null) {
                synchronized (TableStoreRecordReader.class) {
                    if (ots == null) {
                        ots = new SyncClient(ep.endpoint, cred.accessKeyId, cred.accessKeySecret,
                                ep.instance, cred.securityToken);
                    }
                }
            }
        }

        TablestoreSplit tsSplit = ((TableStoreInputSplit) split).getSplit();
        tsSplit.initial(ots);
        results = tsSplit.getRowIterator(
            ots,
            new FilterPushdownConfig(
                filterPushdownConfigSerialize.pushRangeLong,
                filterPushdownConfigSerialize.pushRangeString)
        );
    }

    public static void shutdown() {
        if (ots != null) {
            synchronized (TableStoreRecordReader.class) {
                if (ots != null) {
                    logger.info("shutdown ots client in tablestore record reader");
                    ots.shutdown();
                    ots = null;
                }
            }
        }
    }
}
