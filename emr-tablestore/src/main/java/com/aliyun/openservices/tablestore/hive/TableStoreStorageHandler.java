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
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Map;
import java.util.Set;

import com.aliyun.openservices.tablestore.hadoop.Credential;
import com.aliyun.openservices.tablestore.hadoop.Endpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.ql.plan.TableDesc;

public class TableStoreStorageHandler extends DefaultStorageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(TableStoreStorageHandler.class);

    @Override
    public Class<? extends org.apache.hadoop.mapred.InputFormat<?, ?>> getInputFormatClass() {
        return TableStoreInputFormat.class;
    }

    @Override
    public Class<? extends org.apache.hadoop.mapred.OutputFormat<?, ?>> getOutputFormatClass() {
        return TableStoreOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return TableStoreSerDe.class;
    }

    private Properties getProperties(
            final Configuration conf, final String fileName) throws IOException {
        Path propertiesFilePath = new Path(fileName);
        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
        if (!fs.exists(propertiesFilePath)) {
            throw new IOException(
                "Properties file does not exist: " + fileName);
        }
        FSDataInputStream inputStream = fs.open(propertiesFilePath);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }
        return properties;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        Properties props = tableDesc.getProperties();
        LOG.debug("TableDesc: {}", props);
        String propertiesFilePath = props.getProperty(TableStoreConsts.PROPERTIES_FILE_PATH);
        if (propertiesFilePath != null) {
            try {
                Properties properties = getProperties(getConf(), propertiesFilePath);
                for (Map.Entry<Object, Object> prop : properties.entrySet()) {
                    String key = (String) prop.getKey();
                    String value = (String) prop.getValue();
                    jobProperties.put(key, value);
                }
            } catch (IOException e) {
                LOG.error("Error while trying to read properties file " + propertiesFilePath, e);
            }
        }
        // custom properties has higher priority
        for (String key : TableStoreConsts.REQUIRES) {
            copyToMap(jobProperties, props, key);
            if (jobProperties.get(key) == null) {
                LOG.error("missing required table properties: {}", key);
                throw new IllegalArgumentException("missing required table properties: " + key);
            }
        }

        for (String key : TableStoreConsts.OPTIONALS) {
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

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
                                             Map<String, String> jobProperties) {
        configureInputJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void configureJobConf(TableDesc tableDesc,
                                 org.apache.hadoop.mapred.JobConf jobConf) {
        try {
            Properties from = tableDesc.getProperties();
            LOG.debug("TableDesc: {}", from);
            LOG.debug("job conf: {}", jobConf);
            jobConf.setJarByClass(TableStoreStorageHandler.class);
            String propertiesFilePath = from.getProperty(TableStoreConsts.PROPERTIES_FILE_PATH);
            if (propertiesFilePath != null) {
                try {
                    Properties properties = getProperties(getConf(), propertiesFilePath);
                    for (Map.Entry<Object, Object> prop : properties.entrySet()) {
                        String key = (String) prop.getKey();
                        String value = (String) prop.getValue();
                        from.putIfAbsent(key, value);
                    }
                } catch (IOException e) {
                    LOG.error("Error while trying to read properties file "
                        + propertiesFilePath, e);
                }
            }

            String accessKeyId = from.getProperty(TableStoreConsts.ACCESS_KEY_ID);
            if (accessKeyId == null) {
                LOG.error("{} is required.", TableStoreConsts.ACCESS_KEY_ID);
                throw new IllegalArgumentException(
                    TableStoreConsts.ACCESS_KEY_ID + " is required.");
            }
            String accessKeySecret = from.getProperty(TableStoreConsts.ACCESS_KEY_SECRET);
            if (accessKeySecret == null) {
                LOG.error("{} is required.", TableStoreConsts.ACCESS_KEY_SECRET);
                throw new IllegalArgumentException(
                    TableStoreConsts.ACCESS_KEY_SECRET + " is required.");
            }
            Credential cred = new Credential(accessKeyId, accessKeySecret,
                    from.getProperty(TableStoreConsts.SECURITY_TOKEN));
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setCredential(jobConf, cred);

            String endpoint = from.getProperty(TableStoreConsts.ENDPOINT);
            if (endpoint == null) {
                LOG.error("{} is required.", TableStoreConsts.ENDPOINT);
                throw new IllegalArgumentException(
                    TableStoreConsts.ENDPOINT + " is required.");
            }
            String instance = from.getProperty(TableStoreConsts.INSTANCE);
            Endpoint ep;
            if (instance == null) {
                ep = new Endpoint(endpoint);
            } else {
                ep = new Endpoint(endpoint, instance);
            }
            com.aliyun.openservices.tablestore.hadoop.TableStoreInputFormat
                .setEndpoint(jobConf, ep);

            String table = from.getProperty(TableStoreConsts.TABLE_NAME);
            if (table == null) {
                LOG.error("{} is required.", TableStoreConsts.TABLE_NAME);
                throw new IllegalArgumentException(
                    TableStoreConsts.TABLE_NAME + " is required.");
            }
            com.aliyun.openservices.tablestore.hadoop.TableStoreOutputFormat
                .setOutputTable(jobConf, table);

            String t = from.getProperty(TableStoreConsts.MAX_UPDATE_BATCH_SIZE);
            if (t != null) {
                try {
                    int batchSize = Integer.valueOf(t);
                    if (batchSize <= 0) {
                        LOG.error("{} must be greater than 0.",
                            TableStoreConsts.MAX_UPDATE_BATCH_SIZE);
                        throw new IllegalArgumentException(TableStoreConsts.MAX_UPDATE_BATCH_SIZE
                            + " must be greater than 0.");
                    }
                    com.aliyun.openservices.tablestore.hadoop.TableStoreOutputFormat
                            .setMaxBatchSize(jobConf, batchSize);
                } catch (NumberFormatException ex) {
                    LOG.error("{} must be a positive integer.",
                        TableStoreConsts.MAX_UPDATE_BATCH_SIZE);
                    throw new IllegalArgumentException(TableStoreConsts.MAX_UPDATE_BATCH_SIZE
                        + " must be a positive integer.");
                }
            }

            Set<String> merged = new LinkedHashSet<String>(jobConf.getStringCollection("tmpjars"));
            Job copy = new Job(jobConf);
            TableMapReduceUtils.addDependencyJars(copy);
            merged.addAll(copy.getConfiguration().getStringCollection("tmpjars"));
            jobConf.set("tmpjars", StringUtils.arrayToString(merged.toArray(new String[0])));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
