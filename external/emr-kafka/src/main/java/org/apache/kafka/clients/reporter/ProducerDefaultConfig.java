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

import java.util.HashMap;
import java.util.Map;

public class ProducerDefaultConfig {
    public static final Map<String, Object> PRODUCER_CONFIG_DEFAULTS = new HashMap<String, Object>();
    static {
        PRODUCER_CONFIG_DEFAULTS.put("acks", "all");
        PRODUCER_CONFIG_DEFAULTS.put("compression.type", "lz4");
        PRODUCER_CONFIG_DEFAULTS.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        PRODUCER_CONFIG_DEFAULTS.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        PRODUCER_CONFIG_DEFAULTS.put("linger.ms", "500");
        PRODUCER_CONFIG_DEFAULTS.put("retries", 10);
        PRODUCER_CONFIG_DEFAULTS.put("interceptor.classes", "");
        PRODUCER_CONFIG_DEFAULTS.put("retry.backoff.ms", 500L);
        PRODUCER_CONFIG_DEFAULTS.put("max.in.flight.requests.per.connection", 1);
        PRODUCER_CONFIG_DEFAULTS.put("max.request.size", 10485760);
    }
}
