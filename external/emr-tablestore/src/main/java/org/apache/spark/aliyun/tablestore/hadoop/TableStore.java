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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStore {
    static final String ENDPOINT = "TABLESTORE_ENDPOINT";
    static final String CREDENTIAL = "TABLESTORE_CREDENTIAL";

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
     * for internal use only
     */
    public static SyncClientInterface newOtsClient(Configuration conf) {
        Credential cred = Credential.deserialize(conf.get(TableStore.CREDENTIAL));
        Endpoint ep = Endpoint.deserialize(conf.get(TableStore.ENDPOINT));
        if (cred.securityToken == null) {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
                cred.accessKeySecret, ep.instance);
        } else {
            return new SyncClient(ep.endpoint, cred.accessKeyId,
                cred.accessKeySecret, ep.instance, cred.securityToken);
        }
    }

}

