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

package com.aliyun.openservices.tablestore.spark;

import java.util.List;
import java.util.ArrayList;
import java.util.Formatter;

import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.aliyun.tablestore.hadoop.Credential;
import org.apache.spark.aliyun.tablestore.hadoop.Endpoint;
import org.apache.spark.aliyun.tablestore.hadoop.PrimaryKeyWritable;
import org.apache.spark.aliyun.tablestore.hadoop.RowWritable;
import org.apache.spark.aliyun.tablestore.hadoop.TableStore;
import org.apache.spark.aliyun.tablestore.hadoop.TableStoreInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
            return new SyncClient(ep.endpoint, cred.accessKeyId,
              cred.accessKeySecret, ep.instance);
        } else {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
              cred.accessKeySecret, ep.instance, cred.securityToken);
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
    }


    public static void main(String[] args) {
        if (!parseArgs(args)) {
            printUsage();
            System.exit(1);
        }
        if (endpoint == null || accessKeyId == null || accessKeySecret == null ||
          table == null) {
            printUsage();
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("RowCounter");
        JavaSparkContext sc = null;
        try {
            sc = new JavaSparkContext(sparkConf);
            Configuration hadoopConf = new Configuration();
            TableStore.setCredential(
              hadoopConf,
              new Credential(accessKeyId, accessKeySecret, securityToken));
            Endpoint ep = new Endpoint(endpoint, instance);
            TableStore.setEndpoint(hadoopConf, ep);
            TableStoreInputFormat.addCriteria(hadoopConf, fetchCriteria());

            JavaPairRDD<PrimaryKeyWritable, RowWritable> rdd = sc.newAPIHadoopRDD(
              hadoopConf, TableStoreInputFormat.class, PrimaryKeyWritable.class,
              RowWritable.class);
            System.out.println(
              new Formatter().format("TOTAL: %d", rdd.count()).toString());
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }
}