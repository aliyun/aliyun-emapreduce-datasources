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
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.ecosystem.*;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableStoreInputFormat extends InputFormat<PrimaryKeyWritable, RowWritable> {
    private static final String CRITERIA = "TABLESTORE_CRITERIA";
    public static final String TABLE_NAME = "tableName";
    public static final String COMPUTE_PARAMS = "computeParams";
    public static final String FILTER = "filters";

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreInputFormat.class);

    private static SyncClientInterface ots;
    private static ITablestoreSplitManager splitManager;

    /**
     * Set access-key id/secret into a JobContext.
     * <p>
     * Same as TableStore.setCredential().
     */
    public static void setCredential(JobContext job, String accessKeyId,
                                     String accessKeySecret) {
        TableStore.setCredential(job, accessKeyId, accessKeySecret);
    }

    /**
     * Set access-key id/secret and security token into a JobContext.
     * <p>
     * Same as TableStore.setCredential().
     */
    public static void setCredential(JobContext job, String accessKeyId,
                                     String accessKeySecret, String securityToken) {
        TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
    }

    /**
     * Set credential(access-key id/secret and security token) into a Configuration.
     * <p>
     * Same as TableStore.setCredential().
     */
    public static void setCredential(Configuration conf, Credential cred) {
        TableStore.setCredential(conf, cred);
    }

    /**
     * Set an endpoint of TableStore into a JobContext.
     * <p>
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(JobContext job, String endpoint) {
        TableStore.setEndpoint(job, endpoint);
    }

    /**
     * Set both an endpoint and an instance name into a JobContext.
     * <p>
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(JobContext job, String endpoint, String instance) {
        TableStore.setEndpoint(job, endpoint, instance);
    }

    /**
     * Set both an endpoint and an instance name into a JobContext.
     * <p>
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(Configuration conf, Endpoint ep) {
        TableStore.setEndpoint(conf, ep);
    }

    /**
     * Add a RangeRowQueryCriteria object as data source.
     */
    public static void addCriteria(JobContext job, RangeRowQueryCriteria criteria) {
        Preconditions.checkNotNull(job, "job must be nonnull");
        addCriteria(job.getConfiguration(), criteria);
    }

    /**
     * Add a RangeRowQueryCriteria object as data source.
     */
    public static void addCriteria(Configuration conf, RangeRowQueryCriteria criteria) {
        Preconditions.checkNotNull(criteria, "criteria must be nonnull");
        Preconditions.checkArgument(
                criteria.getDirection() == Direction.FORWARD,
                "criteria must be forward");
        String cur = conf.get(CRITERIA);
        MultiCriteria cri = null;
        if (cur == null) {
            cri = new MultiCriteria();
        } else {
            cri = MultiCriteria.deserialize(cur);
        }
        cri.addCriteria(criteria);
        conf.set(CRITERIA, cri.serialize());
        conf.set(TABLE_NAME, criteria.getTableName());
    }

    /**
     * Clear TableStore data sources.
     */
    public static void clearCriteria(JobContext job) {
        Preconditions.checkNotNull(job, "job must be nonnull");
        clearCriteria(job.getConfiguration());
    }

    /**
     * Clear TableStore data sources.
     */
    public static void clearCriteria(Configuration conf) {
        Preconditions.checkNotNull(conf, "conf must be nonnull");
        conf.unset(CRITERIA);
    }

    @Override
    public RecordReader<PrimaryKeyWritable, RowWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TableStoreRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext job)
            throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        if (ots == null) {
            synchronized (TableStoreInputFormat.class) {
                if (ots == null) {
                    LOG.info("Initial ots client in tablestore inputformat");
                    ots = TableStore.newOtsClient(conf);
                }
            }
        }
        return getSplits(conf, ots);
    }

    /**
     * for internal usage only
     */
    public static List<InputSplit> getSplits(Configuration conf, SyncClientInterface syncClient) {
        Filter filter = new Filter(Filter.CompareOperator.EMPTY_FILTER);
        List<String> requiredColumns = new ArrayList<>();
        if (conf.get(FILTER) != null) {
            TableStoreFilterWritable origFilter = TableStoreFilterWritable.deserialize(conf.get(FILTER));
            if (origFilter != null) {
                filter = origFilter.getFilter();
                requiredColumns = origFilter.getRequiredColumns();
                LOG.info("Set customed filter and requiredColumns: {}", requiredColumns);
            }
        }

        ComputeParameters computeParams = new ComputeParameters();
        if (conf.get(COMPUTE_PARAMS) != null) {
            ComputeParams cp = ComputeParams.deserialize(conf.get(COMPUTE_PARAMS));
            ComputeParameters.ComputeMode computeMode = ComputeParameters.ComputeMode.valueOf(cp.getComputeMode());
            LOG.info("Compute mode: {}, max splits: {}, split size: {}MB, seachIndexName: {}",
                    cp.getComputeMode(), cp.getMaxSplitsCount(), cp.getSplitSizeInMBs(), cp.getSearchIndexName());
            if (computeMode == ComputeParameters.ComputeMode.Search && !cp.getSearchIndexName().isEmpty()) {
                LOG.info("Generate Search compute parameters");
                computeParams = new ComputeParameters(cp.getSearchIndexName(), cp.getMaxSplitsCount());
            } else {
                computeParams = new ComputeParameters(cp.getMaxSplitsCount(), cp.getSplitSizeInMBs(), computeMode);
            }
        }

        if (splitManager == null) {
            synchronized (TableStoreInputFormat.class) {
                LOG.info("Initial split manager in tablestore inputformat");
                splitManager = new DefaultTablestoreSplitManager((SyncClient) syncClient);
            }
        }
        List<ITablestoreSplit> splits = splitManager.generateTablestoreSplits(
                (SyncClient) syncClient, filter, conf.get(TABLE_NAME), computeParams, requiredColumns);

        List<InputSplit> inputSplits = new ArrayList<InputSplit>();
        for (ITablestoreSplit split : splits) {
            inputSplits.add(new TableStoreInputSplit((TablestoreSplit) split));
        }
        LOG.info("Generate {} splits", inputSplits.size());
        return inputSplits;
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
