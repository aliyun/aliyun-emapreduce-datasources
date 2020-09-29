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

import java.util.concurrent.*;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.AlwaysRetryStrategy;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStore {
    public static final String ENDPOINT = "TABLESTORE_ENDPOINT";
    public static final String CREDENTIAL = "TABLESTORE_CREDENTIAL";
    public static final String FILTER_PUSHDOWN_CONFIG = "FILTER_PUSHDOWN_CONFIG";

    /**
     * Set access-key id/secret into a JobContext.
     */
    public static void setCredential(JobContext job, String accessKeyId,
                                     String accessKeySecret) {
        Preconditions.checkNotNull(job, "job must be nonnull");
        setCredential(job.getConfiguration(),
                new Credential(accessKeyId, accessKeySecret, null));
    }

    /**
     * Set access-key id/secret and security token into a JobContext.
     */
    public static void setCredential(JobContext job, String accessKeyId,
                                     String accessKeySecret, String securityToken) {
        Preconditions.checkNotNull(job, "job must be nonnull");
        setCredential(job.getConfiguration(),
                new Credential(accessKeyId, accessKeySecret, securityToken));
    }

    /**
     * Set credential(access-key id/secret and security token) into a Configuration.
     */
    public static void setCredential(Configuration conf, Credential cred) {
        Preconditions.checkNotNull(conf, "conf must be nonnull");
        Preconditions.checkNotNull(cred, "cred must be nonnull");
        conf.set(CREDENTIAL, cred.serialize());
    }

    /**
     * Set an endpoint of TableStore into a JobContext.
     */
    public static void setEndpoint(JobContext job, String endpoint) {
        Preconditions.checkNotNull(job, "job must be nonnull");
        setEndpoint(job.getConfiguration(), new Endpoint(endpoint));
    }

    /**
     * Set both an endpoint and an instance name into a JobContext.
     */
    public static void setEndpoint(JobContext job, String endpoint, String instance) {
        Preconditions.checkNotNull(job, "job must be nonnull");
        setEndpoint(job.getConfiguration(), new Endpoint(endpoint, instance));
    }

    /**
     * Set an endpoint(with/without instance name) into a Configuration.
     */
    public static void setEndpoint(Configuration conf, Endpoint ep) {
        Preconditions.checkNotNull(conf, "conf must be nonnull");
        Preconditions.checkNotNull(ep, "ep must be nonnull");
        conf.set(ENDPOINT, ep.serialize());
    }

    /**
     * Set an endpoint(with/without instance name) into a Configuration.
     */
    public static void setFilterPushdownConfig(Configuration conf, FilterPushdownConfigSerialize filterPushdownConfigSerialize) {
        Preconditions.checkNotNull(conf, "conf must be nonnull");
        if (filterPushdownConfigSerialize == null) {
            filterPushdownConfigSerialize = new FilterPushdownConfigSerialize(true, true);
        }
        conf.set(FILTER_PUSHDOWN_CONFIG, filterPushdownConfigSerialize.serialize());
    }

    /**
     * for internal use only
     */

    public static SyncClientInterface newOtsClient(Configuration conf) {
        Credential cred = Credential.deserialize(conf.get(TableStore.CREDENTIAL));
        Endpoint ep = Endpoint.deserialize(conf.get(TableStore.ENDPOINT));
        ClientConfiguration clientCfg = new ClientConfiguration();
        clientCfg.setRetryStrategy(new DefaultRetryStrategy(10, TimeUnit.SECONDS));
        if (cred.securityToken == null) {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
                    cred.accessKeySecret, ep.instance, clientCfg);
        } else {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
                    cred.accessKeySecret, ep.instance, clientCfg, cred.securityToken);
        }
    }

    public static TableStoreWriter newOtsWriter(Configuration conf) {
        Credential cred = Credential.deserialize(conf.get(TableStore.CREDENTIAL));
        Endpoint ep = Endpoint.deserialize(conf.get(TableStore.ENDPOINT));
        String tableName = conf.get(TableStoreOutputFormat.OUTPUT_TABLE);
        SinkConfig sinkConfig = SinkConfig.deserialize(conf.get(TableStoreOutputFormat.SINK_CONFIG));

        // Client Configurations
        ClientConfiguration cc = new ClientConfiguration();
        if ("time".equals(sinkConfig.getClientRetryStrategy())) {
            cc.setRetryStrategy(new DefaultRetryStrategy(sinkConfig.getClientRetryTime(), TimeUnit.SECONDS));
        } else if ("count".equals(sinkConfig.getClientRetryStrategy())) {
            cc.setRetryStrategy(new AlwaysRetryStrategy(sinkConfig.getClientRetryCount(),
                    sinkConfig.getClientRetryPause()));
        }
        cc.setIoThreadCount(sinkConfig.getClientIoPool());

        // Credentials
        ServiceCredentials credentials =
                CredentialsProviderFactory.newDefaultCredentialProvider(cred.accessKeyId,
                        cred.accessKeySecret, cred.securityToken).getCredentials();

        // Writer Configurations
        WriterConfig wc = new WriterConfig();
        if ("bulk_import".equals(sinkConfig.getWriterBatchRequestType())) {
            wc.setBatchRequestType(BatchRequestType.BULK_IMPORT);
        } else {
            wc.setBatchRequestType(BatchRequestType.BATCH_WRITE_ROW);
        }
        if (sinkConfig.isWriterBatchOrderGuaranteed()) {
            wc.setWriteMode(WriteMode.SEQUENTIAL);
            wc.setAllowDuplicatedRowInBatchRequest(false);
        } else {
            wc.setWriteMode(WriteMode.PARALLEL);
            wc.setAllowDuplicatedRowInBatchRequest(sinkConfig.isWriterBatchDuplicateAllowed());
        }

        wc.setBucketCount(sinkConfig.getWriterBucketNum());
        wc.setCallbackThreadCount(sinkConfig.getWriterCallbackPoolNum());
        wc.setCallbackThreadPoolQueueSize(sinkConfig.getWriterCallbackPoolQueueSize());
        wc.setConcurrency(sinkConfig.getWriterConcurrency());
        wc.setBufferSize(sinkConfig.getWriterBufferSize());
        wc.setFlushInterval(sinkConfig.getWriterFlushIntervalMs());

        wc.setMaxBatchSize(sinkConfig.getWriterMaxBatchSize());
        wc.setMaxBatchRowsCount(sinkConfig.getWriterMaxBatchCount());
        wc.setMaxColumnsCount(sinkConfig.getWriterMaxColumnCount());
        wc.setMaxAttrColumnSize(sinkConfig.getWriterMaxAttrSize());
        wc.setMaxPKColumnSize(sinkConfig.getWriterMaxPkSize());

        TableStoreWriter tablestoreWriter = new DefaultTableStoreWriter(ep.endpoint, credentials,
                ep.instance, tableName, wc, cc,
                // Fake Callback
                new TableStoreCallback<RowChange, RowWriteResult>() {
                    @Override
                    public void onCompleted(RowChange req, RowWriteResult res) {
                    }

                    @Override
                    public void onFailed(RowChange req, Exception ex) {
                    }
                });

        return tablestoreWriter;
    }

    public static void shutdown() {
        TableStoreInputFormat.shutdown();
        TableStoreRecordReader.shutdown();
    }
}

