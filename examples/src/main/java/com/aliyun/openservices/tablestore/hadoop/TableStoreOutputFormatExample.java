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
import java.util.Map;
import java.util.Collections;

import org.apache.spark.aliyun.tablestore.hadoop.BatchWriteWritable;
import org.apache.spark.aliyun.tablestore.hadoop.Credential;
import org.apache.spark.aliyun.tablestore.hadoop.Endpoint;
import org.apache.spark.aliyun.tablestore.hadoop.PrimaryKeyWritable;
import org.apache.spark.aliyun.tablestore.hadoop.RowWritable;
import org.apache.spark.aliyun.tablestore.hadoop.TableStore;
import org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat;
import org.apache.spark.aliyun.tablestore.hadoop.TableStoreOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;

public class TableStoreOutputFormatExample {
  private static final Logger logger = LoggerFactory.getLogger(TableStoreOutputFormatExample.class);
  private static String endpoint;
  private static String accessKeyId;
  private static String accessKeySecret;
  private static String securityToken;
  private static String instance;
  private static String inputTable;
  private static String outputTable;

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
      } else if (args[i].equals("--input-table")) {
        inputTable = args[i+1];
        i += 2;
      } else if (args[i].equals("--security-token")) {
        securityToken = args[i+1];
        i += 2;
      } else if (args[i].equals("--output-table")) {
        outputTable = args[i+1];
        i += 2;
      } else {
        return false;
      }
    }
    return true;
  }

  private static void printUsage() {
    System.err.println("Usage: RowCounter [options]");
    System.err.println("--access-key-id\t\taccess-key id");
    System.err.println("--access-key-secret\taccess-key secret");
    System.err.println("--security-token\t(optional) security token");
    System.err.println("--endpoint\tendpoint");
    System.err.println("--instance\tinstance name");
    System.err.println("--input-table\tinput table name, which must be a \"pet\" table. See docs about TableStoreOutputFormat for details.");
    System.err.println("--output-table\tinput table name, which must be a \"owner-pet\" table. See docs about TableStoreOutputFormat for details.");
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
        new DescribeTableRequest(inputTable));
      return resp.getTableMeta();
    } finally {
      ots.shutdown();
    }
  }

  private static RangeRowQueryCriteria fetchCriteria() {
    TableMeta meta = fetchTableMeta();
    RangeRowQueryCriteria res = new RangeRowQueryCriteria(inputTable);
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

  public static class OwnerMapper
    extends Mapper<PrimaryKeyWritable, RowWritable, Text, MapWritable> {
    @Override public void map(PrimaryKeyWritable key, RowWritable row,
                              Context context) throws IOException, InterruptedException {
      PrimaryKeyColumn pet = key.getPrimaryKey().getPrimaryKeyColumn("name");
      Column owner = row.getRow().getLatestColumn("owner");
      Column species = row.getRow().getLatestColumn("species");
      MapWritable m = new MapWritable();
      m.put(new Text(pet.getValue().asString()),
        new Text(species.getValue().asString()));
      context.write(new Text(owner.getValue().asString()), m);
    }
  }

  public static class IntoTableReducer
    extends Reducer<Text, MapWritable, Text, BatchWriteWritable> {

    @Override public void reduce(Text owner, Iterable<MapWritable> pets,
                                 Context context) throws IOException, InterruptedException {
      List<PrimaryKeyColumn> pkeyCols = new ArrayList<PrimaryKeyColumn>();
      pkeyCols.add(new PrimaryKeyColumn("owner",
        PrimaryKeyValue.fromString(owner.toString())));
      PrimaryKey pkey = new PrimaryKey(pkeyCols);
      List<Column> attrs = new ArrayList<Column>();
      for(MapWritable petMap: pets) {
        for(Map.Entry<Writable, Writable> pet: petMap.entrySet()) {
          Text name = (Text) pet.getKey();
          Text species = (Text) pet.getValue();
          attrs.add(new Column(name.toString(),
            ColumnValue.fromString(species.toString())));
        }
      }
      RowPutChange putRow = new RowPutChange(outputTable, pkey)
        .addColumns(attrs);
      BatchWriteWritable batch = new BatchWriteWritable();
      batch.addRowChange(putRow);
      context.write(owner, batch);
    }
  }


  public static void main(String[] args) throws Exception {
    if (!parseArgs(args)) {
      printUsage();
      System.exit(1);
    }
    if (endpoint == null || accessKeyId == null || accessKeySecret == null ||
      inputTable == null || outputTable == null) {
      printUsage();
      System.exit(1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, TableStoreOutputFormatExample.class.getName());
    job.setMapperClass(OwnerMapper.class);
    job.setReducerClass(IntoTableReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setInputFormatClass(TableStoreInputFormat.class);
    job.setOutputFormatClass(TableStoreOutputFormat.class);

    TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
    TableStore.setEndpoint(job, endpoint, instance);
    TableStoreInputFormat.addCriteria(job, fetchCriteria());
    TableStoreOutputFormat.setOutputTable(job, outputTable);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}