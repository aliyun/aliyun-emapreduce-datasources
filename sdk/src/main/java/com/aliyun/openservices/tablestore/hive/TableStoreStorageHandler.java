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

import java.util.Properties;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class TableStoreStorageHandler extends DefaultStorageHandler {
    private static Logger logger = LoggerFactory.getLogger(TableStoreStorageHandler.class);
    
    @Override public Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass() {
        return TableStoreInputFormat.class;
    }

    @Override public Class<? extends org.apache.hadoop.mapred.OutputFormat> getOutputFormatClass() {
        return TableStoreOutputFormat.class;
    }

    @Override public Class<? extends SerDe> getSerDeClass() {
        return TableStoreSerDe.class;
    }

    @Override public void configureInputJobProperties(TableDesc tableDesc,
        Map<String,String> jobProperties) {
        Properties props = tableDesc.getProperties();
        logger.debug("TableDesc: {}", props);
        for(String key: TableStoreConsts.REQUIRES) {
            String val = copyToMap(jobProperties, props, key);
            if (val == null) {
                logger.error("missing required table properties: {}", key);
                throw new IllegalArgumentException("missing required table properties: " + key);
            }
        }
        for(String key: TableStoreConsts.OPTIONALS) {
            copyToMap(jobProperties, props, key);
        }
    }

    private static String copyToMap(Map<String, String> to, Properties from,
        String key) {
        String val = from.getProperty(key);
        if (val != null) {
            to.put(key, val);
        }
        return val;
    }
    
    @Override public void configureOutputJobProperties(TableDesc tableDesc,
        Map<String,String> jobProperties) {
        configureInputJobProperties(tableDesc, jobProperties);
    }

    @Override public void configureTableJobProperties(TableDesc tableDesc,
        Map<String,String> jobProperties) {
        throw new UnsupportedOperationException();
    }

    @Override public void configureJobConf(TableDesc tableDesc,
        org.apache.hadoop.mapred.JobConf jobConf) {
        Properties from = tableDesc.getProperties();
        logger.debug("TableDesc: {}", from);
        logger.debug("job conf: {}", jobConf);
        jobConf.setJarByClass(TableStoreStorageHandler.class);
        {
            com.aliyun.openservices.tablestore.hadoop.Credential cred =
                new com.aliyun.openservices.tablestore.hadoop.Credential(
                    from.getProperty(TableStoreConsts.ACCESS_KEY_ID),
                    from.getProperty(TableStoreConsts.ACCESS_KEY_SECRET),
                    from.getProperty(TableStoreConsts.SECURITY_TOKEN));
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setCredential(jobConf, cred);
        }
        {
            String endpoint = from.getProperty(TableStoreConsts.ENDPOINT);
            String instance = from.getProperty(TableStoreConsts.INSTANCE);
            com.aliyun.openservices.tablestore.hadoop.Endpoint ep;
            if (instance == null) {
                ep = new com.aliyun.openservices.tablestore.hadoop.Endpoint(
                    endpoint);
            } else {
                ep = new com.aliyun.openservices.tablestore.hadoop.Endpoint(
                    endpoint, instance);
            }
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setEndpoint(jobConf, ep);
        }
        {
            String table = from.getProperty(TableStoreConsts.TABLE_NAME);
            com.aliyun.openservices.tablestore.hadoop.TableStoreOutputFormat
                .setOutputTable(jobConf, table);
        }
    }

}
