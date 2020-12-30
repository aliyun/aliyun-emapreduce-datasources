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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.util.regex.Pattern;

public class RegexMetricPredicate {
    private Pattern pattern = null;

    public RegexMetricPredicate(String regex)
    {
        this.pattern = Pattern.compile(regex);
    }

    public boolean matches(MetricName name, KafkaMetric metric) {
        Boolean excluded = pattern.matcher(name.name()).matches();
        // Collect client aggregated metrics only.
        // So we need to exclude topic-level or broker-level metrics, including:
        // 1. exclude metrics whose `tags` contain "topic" and "node-id".
        // 2. exclude metrics whose MetricName format is '{topic}-{partition}.xxx',
        //    like test-1.records-lag-avg.
        Boolean topicLevelMetrics = name.tags().containsKey("topic")
            || name.tags().containsKey("node-id")
            || name.name().contains(".");
        return !excluded && !topicLevelMetrics;
    }
}
