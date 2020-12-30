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

package com.aliyun.openservices.tablestore.hive;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import com.alicloud.openservices.tablestore.ecosystem.Filter;
import com.aliyun.openservices.tablestore.hadoop.*;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.shims.ShimLoader;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreInputFormat implements InputFormat<PrimaryKeyWritable, RowWritable> {
    private static Logger LOG = LoggerFactory.getLogger(TableStoreInputFormat.class);
    private static SyncClientInterface ots;

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
            throws IOException {
        Configuration dest = translateConfig(job);
        if (ots == null) {
            synchronized (TableStoreInputFormat.class) {
                if (ots == null) {
                    LOG.info("Initial ots client in tablestore inputformat");
                    ots = TableStore.newOtsClient(dest);
                }
            }
        }
        String columns = job.get(TableStoreConsts.COLUMNS_MAPPING);
        if (columns == null) {
            columns = job.get(serdeConstants.LIST_COLUMNS);
        }
        LOG.debug("columns to get: {}", columns);
        List<org.apache.hadoop.mapreduce.InputSplit> splits;
        TableMeta meta = fetchTableMeta(ots,
                job.get(TableStoreConsts.TABLE_NAME));
        RangeRowQueryCriteria criteria = fetchCriteria(meta, columns);
        com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .addCriteria(dest, criteria);
        dest.set(TableStoreConsts.FILTER, new TableStoreFilterWritable(
                new Filter(Filter.CompareOperator.EMPTY_FILTER),
                Arrays.asList(columns.split(","))).serialize());
        splits = com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .getSplits(dest, ots);
        InputSplit[] res = new InputSplit[splits.size()];
        JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(new Job(job));
        Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);
        int i = 0;
        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
            res[i] = new TableStoreInputSplit(
                    (com.aliyun.openservices.tablestore.hadoop.TableStoreInputSplit) split,
                    tablePaths[0]);
            ++i;
        }
        return res;
    }

    private static Configuration translateConfig(Configuration from) {
        Configuration to = new Configuration();
        {
            com.aliyun.openservices.tablestore.hadoop.Credential cred =
                    new com.aliyun.openservices.tablestore.hadoop.Credential(
                            from.get(TableStoreConsts.ACCESS_KEY_ID),
                            from.get(TableStoreConsts.ACCESS_KEY_SECRET),
                            from.get(TableStoreConsts.SECURITY_TOKEN));
            TableStore.setCredential(to, cred);
        }
        {
            String endpoint = from.get(TableStoreConsts.ENDPOINT);
            String instance = from.get(TableStoreConsts.INSTANCE);
            com.aliyun.openservices.tablestore.hadoop.Endpoint ep;
            if (instance == null) {
                ep = new com.aliyun.openservices.tablestore.hadoop.Endpoint(
                        endpoint);
            } else {
                ep = new com.aliyun.openservices.tablestore.hadoop.Endpoint(
                        endpoint, instance);
            }
            TableStore.setEndpoint(to, ep);
            TableStore.setTableName(to, from.get(TableStoreConsts.TABLE_NAME));
        }
        {
            ComputeParams computeParams = new ComputeParams(
                    from.getInt(TableStoreConsts.MAX_SPLIT_COUNT, 1000),
                    from.getLong(TableStoreConsts.SPLIT_SIZE_MBS, 100),
                    from.getTrimmed(TableStoreConsts.COMPUTE_MODE, "KV"));
            TableStore.setComputeParams(to, computeParams);
        }
        return to;
    }

    private static RangeRowQueryCriteria fetchCriteria(TableMeta meta, String strColumns) {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(meta.getTableName());
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for (PrimaryKeySchema schema : meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        res.addColumnsToGet(strColumns.split(","));
        return res;
    }

    private static TableMeta fetchTableMeta(SyncClientInterface ots, String table) {
        DescribeTableResponse resp = ots.describeTable(
                new DescribeTableRequest(table));
        return resp.getTableMeta();
    }

    @Override
    public RecordReader<PrimaryKeyWritable, RowWritable> getRecordReader(
            InputSplit split, JobConf job, Reporter reporter) throws IOException {
        Preconditions.checkNotNull(split, "split must be nonnull");
        Preconditions.checkNotNull(job, "job must be nonnull");
        Preconditions.checkArgument(
                split instanceof TableStoreInputSplit,
                "split must be an instance of " + TableStoreInputSplit.class.getName());
        TableStoreInputSplit tsSplit = (TableStoreInputSplit) split;
        Configuration conf;
        if (isHiveConfiguration(job)) {
            // map task, such as 'select *'
            conf = translateConfig(job);
        } else {
            // reduce task, such as 'select count(*)'
            conf = job;
        }
        final com.aliyun.openservices.tablestore.hadoop.TableStoreRecordReader rdr =
                new com.aliyun.openservices.tablestore.hadoop.TableStoreRecordReader();
        rdr.initialize(tsSplit.getDelegated(), conf);
        return new RecordReader<PrimaryKeyWritable, RowWritable>() {
            @Override
            public boolean next(PrimaryKeyWritable key,
                                RowWritable value) throws IOException {
                boolean next = rdr.nextKeyValue();
                if (next) {
                    key.setPrimaryKey(rdr.getCurrentKey().getPrimaryKey());
                    value.setRow(rdr.getCurrentValue().getRow());
                }
                return next;
            }

            @Override
            public PrimaryKeyWritable createKey() {
                return new PrimaryKeyWritable();
            }

            @Override
            public RowWritable createValue() {
                return new RowWritable();
            }

            @Override
            public long getPos() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                rdr.close();
            }

            @Override
            public float getProgress() throws IOException {
                return rdr.getProgress();
            }
        };
    }

    private boolean isHiveConfiguration(Configuration conf) {
        String endpoint = conf.get(TableStoreConsts.ENDPOINT);
        return endpoint != null;
    }

    public static void shutdown() {
        if (ots != null) {
            synchronized (TableStoreInputFormat.class) {
                if (ots != null) {
                    LOG.info("shutdown ots client in tablestore inputformat");
                    ots.shutdown();
                    ots = null;
                }
            }
        }
    }
}
