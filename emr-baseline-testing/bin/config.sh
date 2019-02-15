#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -o pipefail

SF=10
WAREHOUSE_DIR=hdfs:///user/hive/warehouse
STORE=hdfs
PORT=10001
DB=tpcds_${STORE}_text_${SF}
TABLES=store_returns,store_sales,web_returns,web_sales,inventory,catalog_returns,catalog_sales
BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
CHECKPOINT_ROOT=hdfs:///user/spark/sql/streaming/checkpoint
THROUGHPUT=10000
TESTING_TIMEOUT_MS=120000
TESTING_ENABLE=true
NUM_EXECUTORS=2
EXECUTOR_CORES=2
EXECUTOR_MEMORY=1g