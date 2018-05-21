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

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.clients.reporter.Constant.*;

public class EMRClientMetricsReporter implements MetricsReporter, ClusterResourceListener {
    private static final Logger log = LoggerFactory.getLogger(EMRClientMetricsReporter.class);

    private final Map<MetricName, KafkaMetric> metricMap;
    private KafkaProducer<byte[], byte[]> producer;
    private EMRMetricsReporterConfig emrMetricsReporterConfig;
    private long reportIntervalMs;
    private final ScheduledThreadPoolExecutor executor;
    private volatile String clusterId;
    private String metricsTopic;
    private boolean createTopic;
    private String groupId = "";
    private MetricsType metricType;
    private RegexMetricPredicate predicate;
    private String processInfo;
    private String ip;
    private boolean started = false;

    public EMRClientMetricsReporter() {
        this.executor = new ScheduledThreadPoolExecutor(1);
        this.metricMap = new ConcurrentHashMap<>();
        this.clusterId = null;
        try {
            this.ip = InetAddress.getLocalHost().getHostAddress();
            this.processInfo = ManagementFactory.getRuntimeMXBean().getName();
        } catch (UnknownHostException e) {
            this.ip = "127.0.0.1";
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        log.debug("initializing");
        for (KafkaMetric m : metrics) {
            this.metricMap.put(m.metricName(), m);
        }
        this.executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.executor.setThreadFactory(new ThreadFactory() {
            public Thread newThread(Runnable runnable)
            {
                return EMRClientMetricsReporter.newThread("emr-metrics-reporter-scheduler", runnable, true);
            }
        });
    }

    private static Thread newThread(String name, Runnable runnable, boolean daemon)
    {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e)
            {
                log.error("Uncaught exception in thread '{}':", t.getName(), e);
            }
        });
        return thread;
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        this.metricMap.put(metric.metricName(), metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        log.debug("removing kafka metric : {}", metric.metricName());
        this.metricMap.remove(metric.metricName());
    }

    @Override
    public void close() {
        log.info("Stopping EMR kafka client metrics reporter");
        this.executor.shutdownNow();
        if (this.producer != null) {
            synchronized (this.producer)
            {
                this.producer.close(0L, TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.producer = new KafkaProducer<>(EMRMetricsReporterConfig.producerProperties(configs));
        this.emrMetricsReporterConfig = new EMRMetricsReporterConfig(configs);
        this.predicate = new RegexMetricPredicate(emrMetricsReporterConfig.getString(EMR_METRICS_REPORTER_EXCLUDE_REGEX));
        this.reportIntervalMs = this.emrMetricsReporterConfig.getLong(EMR_METRICS_REPORTER_UPLOAD_INTERVAL_MS);
        this.metricsTopic = this.emrMetricsReporterConfig.getString(EMR_METRICS_REPORTER_TOPIC);
        this.createTopic = this.emrMetricsReporterConfig.getBoolean(EMR_METRICS_REPORTER_TOPIC_CREATE);

        if (configs.containsKey("key.deserializer")) {
            this.metricType = MetricsType.CONSUMER;
        } else if (configs.containsKey("key.serializer")) {
            this.metricType = MetricsType.PRODUCER;
        } else {
            this.metricType = MetricsType.UNKNOWN;
        }

        this.groupId = ((this.metricType == MetricsType.CONSUMER) && (configs.containsKey("group.id")) ? (String)configs.get("group.id") : "");
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        if (!this.started) {
            this.clusterId = clusterResource.clusterId();
            log.info("Starting EMR metrics reporter for cluster id {} with an interval of {} ms", this.clusterId,
                this.reportIntervalMs);
            this.executor.scheduleAtFixedRate(new MetricsReportWatcher(), this.reportIntervalMs, this.reportIntervalMs,
                TimeUnit.MILLISECONDS);
            this.started = true;
        }
    }

    private boolean createTopicIfNotExists() {
        int metricsTopicReplicas = emrMetricsReporterConfig.getInt(EMR_METRICS_REPORTER_TOPIC_REPLICAS);
        int metricsTopicPartitions = emrMetricsReporterConfig.getInt(EMR_METRICS_REPORTER_TOPIC_PARTITIONS);
        long retentionMs = emrMetricsReporterConfig.getLong(EMR_METRICS_REPORTER_TOPIC_RETENTION_MS);
        long retentionBytes = emrMetricsReporterConfig.getLong(EMR_METRICS_REPORTER_TOPIC_RETENTION_BYTES);
        long rollMs = emrMetricsReporterConfig.getLong(EMR_METRICS_REPORTER_TOPIC_ROLL_MS);
        int maxMessageBytes = emrMetricsReporterConfig.getInt(EMR_METRICS_REPORTER_TOPIC_MAX_MESSAGE_BYTES);
        Properties topicConfig = new Properties();
        int minIsr = Math.min(3, metricsTopicReplicas < 3 ? 1 : metricsTopicReplicas - 1);
        topicConfig.put("min.insync.replicas", String.valueOf(minIsr));
        topicConfig.put("retention.ms", "" + String.valueOf(retentionMs));
        topicConfig.put("retention.bytes", "" + String.valueOf(retentionBytes));
        topicConfig.put("segment.ms", "" + String.valueOf(rollMs));
        topicConfig.put("max.message.bytes", "" + String.valueOf(maxMessageBytes));
        topicConfig.put("message.timestamp.type", TimestampType.CREATE_TIME.name);

        String zkConnect = emrMetricsReporterConfig.getString(EMR_METRICS_REPORTER_ZOOKEEPER_CONNECT);
        try {
            AdminUtils utils = new AdminUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled());
            utils.createTopic(metricsTopic, metricsTopicPartitions, metricsTopicReplicas, topicConfig);
            log.debug("Metrics reporter topic {} created successfully.", metricsTopic);
            return true;
        } catch (TopicExistsException e1) {
            return true;
        } catch (Throwable t1) {
            log.error("Failed to create metrics topic: " + metricsTopic, t1);
            return false;
        }
    }

    private Iterable<String> createMetricsMessage() {
        Long metricTime = System.currentTimeMillis();
        Collection<String> out = new ArrayList<>();
        Map<String, String> metricMsg = new HashMap<>();
        for(Map.Entry<MetricName, KafkaMetric> entry: metricMap.entrySet()) {
            try {
                MetricName metricName = entry.getKey();
                KafkaMetric metric = entry.getValue();
                if (predicate.matches(metricName, metric)) {
                    metricMsg.put("client.ip", ip);
                    metricMsg.put("client.process", processInfo);
                    metricMsg.put("prefix", metricType.getPrefix());
                    metricMsg.put("timestamp", String.valueOf(metricTime));
                    metricMsg.put("group", metricName.group());
                    metricMsg.put("attribute", metricName.name());
                    metricMsg.put("value", String.valueOf(metric.value()));
                    if (metricType == MetricsType.CONSUMER && !"".equals(groupId)) {
                        metricMsg.put("group.id", groupId);
                    } else if (metricType == MetricsType.CONSUMER) {
                        metricMsg.put("group.id", "empty");
                    }
                    for (Map.Entry<String, String> tag : metricName.tags().entrySet()) {
                        metricMsg.put("tag." + tag.getKey(), tag.getValue());
                    }
                    out.add(metricMsg.toString());
                    metricMsg.clear();
                }
            } catch (Exception e) {
                // ok
            } finally {
                metricMsg.clear();
            }
        }

        return out;
    }

    private class MetricsReportWatcher implements Runnable {
        private boolean metricsTopicExists = false;

        public MetricsReportWatcher() {}

        @Override
        public void run() {
            try {
                if (createTopic) {
                    if (!metricsTopicExists) {
                        metricsTopicExists = createTopicIfNotExists();
                    }

                    if (!metricsTopicExists) {
                        log.error("Failed to create metrics topic.");
                        return;
                    }
                }
                log.info("Begin to publish metrics to " + metricsTopic);
                Iterable<String> metricsMessages = createMetricsMessage();

                synchronized (producer) {
                    if (!Thread.currentThread().isInterrupted()) {
                        for (String msg : metricsMessages) {
                            log.trace("Created metrics message: " + msg);
                            long curTime = System.currentTimeMillis();
                            producer.send(new ProducerRecord<>(metricsTopic, null, curTime, (byte[]) null, msg.getBytes(StandardCharsets.UTF_8)), new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata metadata, Exception exception) {
                                    if (exception != null) {
                                        log.warn("Failed to produce metrics message", exception);
                                    } else {
                                        log.debug("Produced metrics message of size {} with offset {} to topic partition {}-{}",
                                            metadata.serializedValueSize(), metadata.offset(), metadata.topic(), metadata.partition());
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (InterruptException e1) {
                // ok
            } catch (Throwable t1) {
                log.warn("Failed to produce metrics message.", t1);
            }
        }
    }
}
