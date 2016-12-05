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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreInputFormat extends InputFormat<PrimaryKeyWritable, RowWritable> {
    static final String CRITERIA = "TABLESTORE_CRITERIA";

    private static final Logger logger = LoggerFactory.getLogger(TableStoreInputFormat.class);
    
    /**
     * Set access-key id/secret into a JobContext.
     *
     * Same as TableStore.setCredential().
     */
    public static void setCredential(
        JobContext job,
        String accessKeyId,
        String accessKeySecret)
    {
        TableStore.setCredential(job, accessKeyId, accessKeySecret);
    }

    /**
     * Set access-key id/secret and security token into a JobContext.
     *
     * Same as TableStore.setCredential().
     */
    public static void setCredential(
        JobContext job,
        String accessKeyId,
        String accessKeySecret,
        String securityToken)
    {
        TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
    }

    /**
     * Set credential(access-key id/secret and security token) into a Configuration.
     *
     * Same as TableStore.setCredential().
     */
    public static void setCredential(
        Configuration conf,
        Credential cred)
    {
        TableStore.setCredential(conf, cred);
    }

    /**
     * Set an endpoint of TableStore into a JobContext.
     *
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(
        JobContext job,
        String endpoint)
    {
        TableStore.setEndpoint(job, endpoint);
    }

    /**
     * Set both an endpoint and an instance name into a JobContext.
     *
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(
        JobContext job,
        String endpoint,
        String instance)
    {
        TableStore.setEndpoint(job, endpoint, instance);
    }

    /**
     * Set both an endpoint and an instance name into a JobContext.
     *
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
        InputSplit split,
        TaskAttemptContext context)
        throws IOException,
               InterruptedException {
        TableStoreRecordReader rdr = new TableStoreRecordReader();
        rdr.initialize(split, context);
        return rdr;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job)
        throws IOException, InterruptedException
    {
        Configuration conf = job.getConfiguration();
        SyncClientInterface ots = TableStore.newOtsClient(conf);
        try {
            return getSplits(conf, ots);
        } finally {
            ots.shutdown();
        }
    }

    /**
     * for internal usage only
     */
    public static List<InputSplit> getSplits(Configuration conf, SyncClientInterface ots) {
        List<RangeRowQueryCriteria> scans = getScans(conf);
        logger.info("{} scans", scans.size());
        Set<String> tables = collectTables(scans);
        Map<String, List<PrimaryKey>> splits = fetchSplits(ots, tables); // table -> shard split points
        {
            int splitCnt = 0;
            for(List<PrimaryKey> entry: splits.values()) {
                splitCnt += entry.size();
            }
            logger.info("{} split points in {} tables", splitCnt, tables.size());
        }

        List<InputSplit> res = new ArrayList<InputSplit>();
        for(RangeRowQueryCriteria s: scans) {
            String table = s.getTableName();
            List<PrimaryKey> split = splits.get(table);
            Preconditions.checkNotNull(split, "");
            PrimaryKey start = s.getInclusiveStartPrimaryKey();
            int startIdx = locateStart(split, start);
            PrimaryKey end = s.getExclusiveEndPrimaryKey();
            int endIdx = locateEnd(split, end);

            Preconditions.checkArgument(startIdx <= endIdx,
                "inclusive-start primary key must be smaller than exclusive-end primary key for FORWARD direction");
            if (startIdx == endIdx) {
                res.add(new TableStoreInputSplit(s));
            } else {
                for(int i = startIdx; i <= endIdx; ++i) {
                    RangeRowQueryCriteria x = clone(s);
                    if (i > startIdx) {
                        x.setInclusiveStartPrimaryKey(split.get(i - 1));
                    }
                    if (i < endIdx) {
                        x.setExclusiveEndPrimaryKey(split.get(i));
                    }
                    res.add(new TableStoreInputSplit(x));
                }
            }
        }
        logger.info("{} splits", res.size());
        return res;
    }

    private static List<RangeRowQueryCriteria> getScans(Configuration conf) {
        String cur = conf.get(CRITERIA);
        MultiCriteria cri = null;
        if (cur == null) {
            cri = new MultiCriteria();
        } else {
            cri = MultiCriteria.deserialize(cur);
        }
        List<RangeRowQueryCriteria> scans = cri.getCriteria();

        return scans;
    }

    private static Set<String> collectTables(List<RangeRowQueryCriteria> scans)
    {
        Set<String> res = new HashSet<String>();
        for(RangeRowQueryCriteria s: scans) {
            String tbl = s.getTableName();
            res.add(tbl);
        }
        return res;
    }

    private static Map<String, List<PrimaryKey>> fetchSplits(
        SyncClientInterface ots,
        Set<String> tables)
    {
        Map<String, List<PrimaryKey>> res = new HashMap<String, List<PrimaryKey>>();
        for(String tbl: tables) {
            if (res.containsKey(tbl)) {
                continue;
            }
            DescribeTableResponse resp = ots.describeTable(new DescribeTableRequest(tbl));
            List<PrimaryKeySchema> schema = resp.getTableMeta().getPrimaryKeyList();
            List<PrimaryKey> pkeys = new ArrayList<PrimaryKey>();
            for(PrimaryKey pkey: resp.getShardSplits()) {
                PrimaryKeyColumn[] cols = pkey.getPrimaryKeyColumns();
                Preconditions.checkArgument(
                    cols.length <= schema.size(),
                    "# of primary key columns in one shard split is greater than that of table schema");
                if (cols.length == schema.size()) {
                    pkeys.add(pkey);
                } else {
                    PrimaryKeyColumn[] newCols = new PrimaryKeyColumn[schema.size()];
                    System.arraycopy(cols, 0, newCols, 0, cols.length);
                    for(int i = cols.length; i < schema.size(); ++i) {
                        newCols[i] = new PrimaryKeyColumn(schema.get(i).getName(), PrimaryKeyValue.INF_MIN);
                    }
                    pkeys.add(new PrimaryKey(newCols));
                }
            }
            res.put(tbl, pkeys);
        }
        return res;
    }

    private static int locateStart(List<PrimaryKey> points, PrimaryKey p) {
        int pos = Collections.binarySearch(points, p);
        if (pos >= 0) {
            return pos + 1;
        } else {
            return -pos - 1;
        }
    }

    private static int locateEnd(List<PrimaryKey> points, PrimaryKey p) {
        int pos = Collections.binarySearch(points, p);
        if (pos >= 0) {
            return pos;
        } else {
            return -pos - 1;
        }
    }

    private static RangeRowQueryCriteria clone(RangeRowQueryCriteria src) {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(src.getTableName());
        if (src.getLimit() > 0) {
            res.setLimit(src.getLimit());
        }
        res.setDirection(src.getDirection());
        res.setInclusiveStartPrimaryKey(src.getInclusiveStartPrimaryKey());
        res.setExclusiveEndPrimaryKey(src.getExclusiveEndPrimaryKey());
        src.copyTo(res);
        return res;
    }
}
