package com.aliyun.openservices.tablestore.hive;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hive.serde.serdeConstants;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.aliyun.openservices.tablestore.hadoop.PrimaryKeyWritable;
import com.aliyun.openservices.tablestore.hadoop.RowWritable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreInputFormat implements InputFormat<PrimaryKeyWritable, RowWritable> {
    private static Logger logger = LoggerFactory.getLogger(TableStoreInputFormat.class);

    @Override
    public InputSplit[] getSplits(
        JobConf job,
        int numSplits)
        throws IOException
    {
        Configuration dest = translateConfig(job);
        SyncClientInterface ots = null;
        String columns = job.get(TableStoreConsts.COLUMNS_MAPPING);
        if (columns == null) {
            columns = job.get(serdeConstants.LIST_COLUMNS);
        }
        logger.debug("columns to get: {}", columns);
        List<org.apache.hadoop.mapreduce.InputSplit> splits;
        try {
            ots = com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .newOtsClient(dest);
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .addCriteria(
                    dest,
                    fetchCriteria(
                        fetchTableMeta(
                            ots,
                            job.get(TableStoreConsts.TABLE_NAME)),
                        columns));
            splits = com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .getSplits(dest, ots);
        } finally {
            if (ots != null) {
                ots.shutdown();
                ots = null;
            }
        }
        InputSplit[] res = new InputSplit[splits.size()];
        int i = 0;
        for(org.apache.hadoop.mapreduce.InputSplit split: splits) {
            res[i] = new TableStoreInputSplit(
                (com.aliyun.openservices.tablestore.hadoop.TableStoreInputSplit) split);
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
                    from.get(TableStoreConsts.STS_TOKEN));
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setCredential(to, cred);
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
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setEndpoint(to, ep);
        }
        return to;
    }

    private static RangeRowQueryCriteria fetchCriteria(TableMeta meta, String strColumns)
    {
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(meta.getTableName());
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema schema: meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        res.addColumnsToGet(strColumns.split(","));
        return res;
    }

    private static TableMeta fetchTableMeta(
        SyncClientInterface ots,
        String table)
    {
        DescribeTableResponse resp = ots.describeTable(
            new DescribeTableRequest(table));
        return resp.getTableMeta();
    }

    @Override
    public RecordReader<PrimaryKeyWritable, RowWritable> getRecordReader(
        InputSplit split,
        JobConf job,
        Reporter reporter)
        throws IOException
    {
        Preconditions.checkNotNull(split, "split must be nonnull");
        Preconditions.checkNotNull(job, "job must be nonnull");
        Preconditions.checkArgument(
            split instanceof TableStoreInputSplit,
            "split must be one of " + TableStoreInputSplit.class.getName());
        TableStoreInputSplit tsSplit = (TableStoreInputSplit) split;

        Configuration conf = translateConfig(job);
        final com.aliyun.openservices.tablestore.hadoop.TableStoreRecordReader rdr =
            new com.aliyun.openservices.tablestore.hadoop.TableStoreRecordReader();
        rdr.initialize(tsSplit.getDelegated(), conf);
        return new RecordReader<PrimaryKeyWritable, RowWritable>() {
            @Override
            public boolean next(PrimaryKeyWritable key, RowWritable value) throws IOException {
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
}
