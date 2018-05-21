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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import static org.apache.kafka.clients.reporter.Constant.*;

public class EMRMetricsReporterConfig extends AbstractConfig {
    public static final Long DEFAULT_UPLOAD_PERIOD = TimeUnit.SECONDS.toMillis(10L);
    public static final long DEFAULT_TOPIC_ROLL_MS_CONFIG = TimeUnit.HOURS.toMillis(6L);
    public static final long DEFAULT_TOPIC_RETENTION_MS_CONFIG = TimeUnit.DAYS.toMillis(3L);

    private static final ConfigDef config = new ConfigDef()
        .define(EMR_METRICS_REPORTER_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Bootstrap servers for the Kafka client metrics will be written to.")
        .define(EMR_METRICS_REPORTER_ZOOKEEPER_CONNECT, ConfigDef.Type.STRING, "localhost:2181", ConfigDef.Importance.HIGH, "The Zookeeper address for the kafka cluster.")
        .define(EMR_METRICS_REPORTER_TOPIC, ConfigDef.Type.STRING, "_emr-client-metrics", ConfigDef.Importance.LOW, "Topic on which kafka client metrics data will be written to.")
        .define(EMR_METRICS_REPORTER_TOPIC_CREATE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, "Create the kafka client metrics topic if not exist.")
        .define(EMR_METRICS_REPORTER_TOPIC_PARTITIONS, ConfigDef.Type.INT, 50, ConfigDef.Importance.LOW, "Number of partitions of the kafka client metrics topic.")
        .define(EMR_METRICS_REPORTER_TOPIC_REPLICAS, ConfigDef.Type.INT, 2, ConfigDef.Importance.LOW, "Number of replicas of the kafka client metric topic.")
        .define(EMR_METRICS_REPORTER_TOPIC_RETENTION_MS, ConfigDef.Type.LONG, DEFAULT_TOPIC_RETENTION_MS_CONFIG, ConfigDef.Importance.LOW, "Retention time for the client metrics topic.")
        .define(EMR_METRICS_REPORTER_TOPIC_RETENTION_BYTES, ConfigDef.Type.LONG, -1L, ConfigDef.Importance.LOW, "Retention bytes for the client metrics topic.")
        .define(EMR_METRICS_REPORTER_TOPIC_ROLL_MS, ConfigDef.Type.LONG, DEFAULT_TOPIC_ROLL_MS_CONFIG, ConfigDef.Importance.LOW, "Log rolling time for the client metrics topic.")
        .define(EMR_METRICS_REPORTER_TOPIC_MAX_MESSAGE_BYTES, ConfigDef.Type.INT, 10485760, ConfigDef.Range.atLeast(0), ConfigDef.Importance.MEDIUM, "Maximum message size for the client metrics topic.")
        .define(EMR_METRICS_REPORTER_UPLOAD_INTERVAL_MS, ConfigDef.Type.LONG, DEFAULT_UPLOAD_PERIOD, ConfigDef.Importance.LOW, "The client metrics reporter will write new metrics to the client metrics topic in intervals defined by this setting.")
        .define(EMR_METRICS_REPORTER_EXCLUDE_REGEX, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "The client metrics exclude regex, default exclude none.");

    public EMRMetricsReporterConfig(Properties props) {
        super(config, props);
    }

    public EMRMetricsReporterConfig(Map<String, ?> originals) {
        super(config, originals);
    }

    public static Properties clientProperties(Map<String, ?> clientConfigs) {
        Properties props = new Properties();
        for (Map.Entry<String, ?> entry : clientConfigs.entrySet()) {
            if ((entry.getKey()).startsWith(EMR_METRICS_REPORTER_PREFIX)) {
                props.put((entry.getKey()).substring("emr.metrics.reporter.".length()), entry.getValue());
            }
        }
        Object bootstrap = props.get("bootstrap.servers");
        if (bootstrap == null) {
            throw new ConfigException("Missing required property emr.metrics.reporter.bootstrap.servers.");
        }
        return props;
    }

    public static Properties producerProperties(Map<String, ?> clientConfigs) {
        Properties props = new Properties();
        props.putAll(ProducerDefaultConfig.PRODUCER_CONFIG_DEFAULTS);
        props.put("client.id", "emr-client-metrics-reporter");
        props.putAll(clientProperties(clientConfigs));
        return props;
    }
}
