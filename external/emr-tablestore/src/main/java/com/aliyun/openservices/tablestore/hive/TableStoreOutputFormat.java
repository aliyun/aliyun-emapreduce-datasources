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

import java.io.IOException;

import com.aliyun.openservices.tablestore.hadoop.Credential;
import com.aliyun.openservices.tablestore.hadoop.Endpoint;
import com.aliyun.openservices.tablestore.hadoop.TableStoreRecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.aliyun.openservices.tablestore.hadoop.TableStore;
import com.aliyun.openservices.tablestore.hadoop.BatchWriteWritable;
import com.alicloud.openservices.tablestore.SyncClientInterface;

public class TableStoreOutputFormat implements OutputFormat<Writable, BatchWriteWritable> {
    private static Logger logger = LoggerFactory.getLogger(TableStoreOutputFormat.class);

    @Override public RecordWriter<Writable, BatchWriteWritable> getRecordWriter(
        FileSystem ignored, JobConf job, String name, Progressable progress)
        throws IOException {
        String table = job.get(TableStoreConsts.TABLE_NAME);
        Configuration conf = translateConfig(job);
        SyncClientInterface ots = TableStore.newOtsClient(conf);
        final org.apache.hadoop.mapreduce.RecordWriter<Writable, BatchWriteWritable> writer =
            new TableStoreRecordWriter(ots, table);
        return new org.apache.hadoop.mapred.RecordWriter<Writable, BatchWriteWritable>() {
            @Override public void write(Writable any, BatchWriteWritable rows)
                throws IOException {
                try {
                    writer.write(any, rows);
                } catch(InterruptedException ex) {
                    throw new IOException("interrupted");
                }
            }

            @Override public void close(Reporter reporter) throws IOException {
                try {
                    writer.close(null);
                } catch(InterruptedException ex) {
                    throw new IOException("interrupted");
                }
            }
        };
    }

    @Override public void checkOutputSpecs(FileSystem ignored, JobConf job)
        throws IOException {
        Configuration dest = translateConfig(job);
        com.aliyun.openservices.tablestore.hadoop.TableStoreOutputFormat.checkTable(dest);
    }

    private static Configuration translateConfig(Configuration from) {
        Configuration to = new Configuration();
        {
            Credential cred =
                new Credential(
                    from.get(TableStoreConsts.ACCESS_KEY_ID),
                    from.get(TableStoreConsts.ACCESS_KEY_SECRET),
                    from.get(TableStoreConsts.SECURITY_TOKEN));
            TableStore.setCredential(to, cred);
        }
        {
            String endpoint = from.get(TableStoreConsts.ENDPOINT);
            String instance = from.get(TableStoreConsts.INSTANCE);
            Endpoint ep;
            if (instance == null) {
                ep = new Endpoint(
                    endpoint);
            } else {
                ep = new Endpoint(
                    endpoint, instance);
            }
            TableStore.setEndpoint(to, ep);
        }
        {
            com.aliyun.openservices.tablestore.hadoop.TableStoreOutputFormat
                .setOutputTable(to, from.get(TableStoreConsts.TABLE_NAME));
        }
        return to;
    }

}

