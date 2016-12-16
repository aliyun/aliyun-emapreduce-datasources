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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

public class RowCounter {
    private static String endpoint;
    private static String accessKeyId;
    private static String accessKeySecret;
    private static String securityToken;
    private static String instance;
    private static String table;
    private static String outputPath;
    
    public static class RowCounterMapper 
      extends Mapper<PrimaryKeyWritable, RowWritable, Text, LongWritable> {
        private final static Text agg = new Text("TOTAL");
        private final static LongWritable one = new LongWritable(1);

        @Override public void map(PrimaryKeyWritable key, RowWritable value,
            Context context) throws IOException, InterruptedException {
            context.write(agg, one);
        }
    }

    public static class IntSumReducer
      extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override public void reduce(Text key, Iterable<LongWritable> values,
            Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
    
    private static boolean parseArgs(String[] args) {
        for(int i = 0; i < args.length;) {
            if (args[i].equals("--endpoint")) {
                endpoint = args[i+1];
                i += 2;
            } else if (args[i].equals("--access-key-id")) {
                accessKeyId = args[i+1];
                i += 2;
            } else if (args[i].equals("--access-key-secret")) {
                accessKeySecret = args[i+1];
                i += 2;
            } else if (args[i].equals("--instance")) {
                instance = args[i+1];
                i += 2;
            } else if (args[i].equals("--table")) {
                table = args[i+1];
                i += 2;
            } else if (args[i].equals("--security-token")) {
                securityToken = args[i+1];
                i += 2;
            } else if (args[i].equals("--output")) {
                outputPath = args[i+1];
                i += 2;
            } else {
                return false;
            }
        }
        return true;
    }

    private static SyncClient getOTSClient() {
        Credential cred = new Credential(accessKeyId, accessKeySecret, securityToken);
        Endpoint ep;
        if (instance == null) {
            ep = new Endpoint(endpoint);
        } else {
            ep = new Endpoint(endpoint, instance);
        }
        if (cred.securityToken == null) {
            return new SyncClient(
                ep.endpoint, cred.accessKeyId, cred.accessKeySecret, ep.instance);
        } else {
            return new SyncClient(
                ep.endpoint, cred.accessKeyId, cred.accessKeySecret, ep.instance,
                cred.securityToken);
        }
    }

    private static TableMeta fetchTableMeta() {
        SyncClient ots = getOTSClient();
        try {
            DescribeTableResponse resp = ots.describeTable(
                new DescribeTableRequest(table));
            return resp.getTableMeta();
        } finally {
            ots.shutdown();
        }
    }

    private static RangeRowQueryCriteria fetchCriteria() {
        TableMeta meta = fetchTableMeta();
        RangeRowQueryCriteria res = new RangeRowQueryCriteria(table);
        res.setMaxVersions(1);
        List<PrimaryKeyColumn> lower = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upper = new ArrayList<PrimaryKeyColumn>();
        for(PrimaryKeySchema schema: meta.getPrimaryKeyList()) {
            lower.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MIN));
            upper.add(new PrimaryKeyColumn(schema.getName(), PrimaryKeyValue.INF_MAX));
        }
        res.setInclusiveStartPrimaryKey(new PrimaryKey(lower));
        res.setExclusiveEndPrimaryKey(new PrimaryKey(upper));
        return res;
    }

    private static void printUsage() {
        System.err.println("Usage: RowCounter [options]");
        System.err.println("--access-key-id\t\taccess-key id");
        System.err.println("--access-key-secret\taccess-key secret");
        System.err.println("--security-token\t(optional) security token");
        System.err.println("--endpoint\tendpoint");
        System.err.println("--instance\tinstance name");
        System.err.println("--table\ttable name");
        System.err.println("--output\tdirectory to place outputs. this directory must be nonexistent.");
    }

    public static void main(String[] args) throws Exception {
        if (!parseArgs(args)) {
            printUsage();
            System.exit(1);
        }
        if (endpoint == null || accessKeyId == null || accessKeySecret == null ||
            table == null || outputPath == null) {
            printUsage();
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "row count");
        job.setJarByClass(RowCounter.class);
        job.setMapperClass(RowCounterMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TableStoreInputFormat.class);

        TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
        TableStore.setEndpoint(job, endpoint, instance);
        TableStoreInputFormat.addCriteria(job, fetchCriteria());
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

