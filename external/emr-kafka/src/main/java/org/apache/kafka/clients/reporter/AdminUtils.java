/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.reporter;

import java.io.UnsupportedEncodingException;
import java.util.*;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

public class AdminUtils {
    private ZkClient zkClient;
    private List<ACL> defaultAcls;

    public AdminUtils(String zkUrl, int sessionTimeout, int connectionTimeout, boolean isZkSecurityEnabled) {
        ZkConnection zkConnection = new ZkConnection(zkUrl, sessionTimeout);
        this.zkClient = new ZkClient(zkConnection, connectionTimeout, new ZKStringSerializer());
        this.defaultAcls = defaultAcls(isZkSecurityEnabled);
    }

    public void createTopic(String topic, int partitions, int replicationFactor, Properties topicConfig) {
        String topicPath = "/brokers/topics/" + topic;
        try {
            if (zkClient.exists(topicPath)) {
                throw new TopicExistsException("Topic \"%s\" already exists.".format(topic));
            } else {
                writeEntityConfig(topicConfig, topic);
                writeTopicPartitionAssignment(topic, partitions, replicationFactor);
            }
        } finally {
            if (zkClient != null) {
                zkClient.close();
                zkClient = null;
            }
        }
    }

    public void writeEntityConfig(Properties topicConfig, String topic) {
        String configPath = "/config/topics/" + topic;
        StringBuilder sb = new StringBuilder();
        sb.append("{\"version\":1");
        sb.append(",");
        sb.append("\"config\":{");
        Iterator<Map.Entry<Object, Object>> it = topicConfig.entrySet().iterator();
        Map.Entry<Object, Object> entry;
        if (it.hasNext()) {
            entry = it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (!(key instanceof String)) {
                throw new ConfigException(key.toString(), value, "Key should be a string");
            } else {
                sb.append("\"" + key + "\"");
                sb.append(":");
                sb.append("\"" + value.toString() + "\"");
            }
        }
        while (it.hasNext()) {
            entry = it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (!(key instanceof String)) {
                throw new ConfigException(key.toString(), value, "Key should be a string");
            } else {
                sb.append(",");
                sb.append("\"" + key + "\"");
                sb.append(":");
                sb.append("\"" + value.toString() + "\"");
            }
        }
        sb.append("}}");
        String data = sb.toString();
        zkClient.createPersistent(configPath, data, defaultAcls);
    }

    private List<String> getBrokerIds() {
        List<String> brokerIds = zkClient.getChildren("/brokers/ids");
        List<String> validBrokerIds = new ArrayList<>();
        for(String id: brokerIds) {
            try {
                String path = "/brokers/ids/" + id;
                zkClient.readData(path);
                validBrokerIds.add(id);
            } catch (ZkNoNodeException e) {
                // skip
            }
        }
        Collections.sort(validBrokerIds);
        return validBrokerIds;
    }

    private Map<String, int[]> createReplicaAssignment(List<String> brokerIds, int partitions, int replicationFactor) {
        int numBrokers = brokerIds.size();
        Map<String, int[]> replicaAssignment = new HashMap<>();
        for(int i = 0; i < partitions; i++) {
            int[] places = new int[replicationFactor];
            for (int j = 0; j < replicationFactor; j ++) {
                places[j] = Integer.parseInt(brokerIds.get((i + j) % numBrokers));
            }
            replicaAssignment.put(i+"", places);
        }
        return replicaAssignment;
    }

    private void writeTopicPartitionAssignment(String topic, int partitions, int replicationFactor) {
        List<String> brokerIds = getBrokerIds();
        Map<String, int[]> replicaAssignment = createReplicaAssignment(brokerIds, partitions, replicationFactor);
        String topicPath = "/brokers/topics/" + topic;
        StringBuilder sb = new StringBuilder();
        sb.append("{\"version\":1");
        sb.append(",");
        sb.append("\"partitions\":{");
        Iterator<Map.Entry<String, int[]>> it = replicaAssignment.entrySet().iterator();
        Map.Entry<String, int[]> entry;
        if (it.hasNext()) {
            entry = it.next();
            String key = entry.getKey();
            int[] value = entry.getValue();
            sb.append("\"" + key + "\"");
            sb.append(":");
            sb.append(Arrays.toString(value));
        }
        while (it.hasNext()) {
            entry = it.next();
            String key = entry.getKey();
            int[] value = entry.getValue();
            sb.append(",");
            sb.append("\"" + key + "\"");
            sb.append(":");
            sb.append(Arrays.toString(value));
        }
        sb.append("}}");
        String data = sb.toString();
        zkClient.createPersistent(topicPath, data, defaultAcls);
    }

    private static class ZKStringSerializer implements ZkSerializer {
        @Override
        public byte[] serialize(Object o) throws ZkMarshallingError {
            try {
                return ((String) o).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            if (bytes == null) {
                return null;
            } else {
                try {
                    return new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
    }

    private List<ACL> defaultAcls(boolean isSecure) {
        if (isSecure) {
            List<ACL> list = new ArrayList<>();
            list.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
            list.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
            return list;
        } else {
            return ZooDefs.Ids.OPEN_ACL_UNSAFE;
        }
    }

    public static void main(String[] args) {
        AdminUtils utils = new AdminUtils("localhost:2181", 1, 1, false);
        Properties prop = new Properties();
        prop.put("a", 1);
        prop.put("b", false);
        prop.put("c", "aaa");
        utils.writeEntityConfig(prop, "a");
        utils.writeTopicPartitionAssignment("test", 50, 3);
    }
}
