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

package org.apache.spark.aliyun.tablestore.hadoop;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreOutputFormat extends OutputFormat<Writable, BatchWriteWritable> {
    public static final String OUTPUT_TABLE = "TABLESTORE_OUTPUT_TABLE";
    private static final Logger logger = LoggerFactory.getLogger(TableStoreOutputFormat.class);

    /**
     * Set access-key id/secret into a JobContext.
     *
     * Same as TableStore.setCredential().
     */
    public static void setCredential(JobContext job, String accessKeyId,
        String accessKeySecret) {
        TableStore.setCredential(job, accessKeyId, accessKeySecret);
    }

    /**
     * Set access-key id/secret and security token into a JobContext.
     *
     * Same as TableStore.setCredential().
     */
    public static void setCredential(JobContext job, String accessKeyId,
        String accessKeySecret, String securityToken) {
        TableStore.setCredential(job, accessKeyId, accessKeySecret, securityToken);
    }

    /**
     * Set credential(access-key id/secret and security token) into a Configuration.
     *
     * Same as TableStore.setCredential().
     */
    public static void setCredential(Configuration conf, Credential cred) {
        TableStore.setCredential(conf, cred);
    }

    /**
     * Set an endpoint of TableStore into a JobContext.
     *
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(JobContext job, String endpoint) {
        TableStore.setEndpoint(job, endpoint);
    }

    /**
     * Set both an endpoint and an instance name into a JobContext.
     *
     * Same as TableStore.setEndpoint().
     */
    public static void setEndpoint(JobContext job, String endpoint, String instance) {
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
     * Set a table in TableStore as data sink.
     */
    public static void setOutputTable(JobContext job, String outputTable) {
        setOutputTable(job.getConfiguration(), outputTable);
    }

    /**
     * Set a table in TableStore as data sink.
     */
    public static void setOutputTable(Configuration conf, String outputTable) {
        Preconditions.checkNotNull(outputTable, "Output table must be nonnull.");
        conf.set(OUTPUT_TABLE, outputTable);
    }


    @Override public void checkOutputSpecs(JobContext job)
        throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        checkTable(conf);
    }

    public static void checkTable(Configuration conf) throws IOException {
        String outputTable = conf.get(OUTPUT_TABLE);
        Preconditions.checkNotNull(outputTable, "Output table must be nonnull.");
        SyncClientInterface ots = TableStore.newOtsClient(conf);
        try {
            // test existence of the output table
            ots.describeTable(new DescribeTableRequest(outputTable));
            logger.info("{} exists", outputTable);
        } catch(TableStoreException ex) {
            logger.error("{} does not exist, or it is unaccessible.", outputTable);
            logger.error("{}", ex);
            throw new IOException("output table is unaccessible.");
        } finally {
            ots.shutdown();
        }
    }

    @Override public OutputCommitter getOutputCommitter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        return new OutputCommitter() {
            @Override public void abortJob(JobContext jobContext,
                org.apache.hadoop.mapreduce.JobStatus.State state) {
            }

            @Override public void abortTask(TaskAttemptContext taskContext) {
            }

            @Override public void commitJob(JobContext jobContext) {
            }

            @Override public void commitTask(TaskAttemptContext taskContext) {
            }

            @Override public boolean needsTaskCommit(
                TaskAttemptContext taskContext) throws IOException {
                return false;
            }

            @Override public void recoverTask(
                TaskAttemptContext taskContext) throws IOException {
            }

            @Override public void setupJob(JobContext jobContext)
                throws IOException {
            }

            @Override public void setupTask(TaskAttemptContext taskContext)
                throws IOException {
            }
        };
    }

    @Override public RecordWriter<Writable, BatchWriteWritable> getRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String outputTable = conf.get(OUTPUT_TABLE);
        SyncClientInterface ots = TableStore.newOtsClient(conf);
        return new TableStoreRecordWriter(ots, outputTable);
    }
}

