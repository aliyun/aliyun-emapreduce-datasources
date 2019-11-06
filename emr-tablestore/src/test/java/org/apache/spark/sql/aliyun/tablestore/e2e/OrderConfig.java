/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.aliyun.tablestore.e2e;

import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.FileReader;

public class OrderConfig {
    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instanceName;
    private String sourceTable;
    private String sinkTable;

    public static OrderConfig load(String confPath) {
        try {
            BufferedReader bs = new BufferedReader(new FileReader(confPath));

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = bs.readLine()) != null) {
                sb.append(line);
            }
            return JSON.parseObject(sb.toString(), OrderConfig.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new OrderConfig();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public static void main(String[] args) {
        OrderConfig config = OrderConfig.load("config.json");
        System.out.println(JSON.toJSONString(config));
    }
}
