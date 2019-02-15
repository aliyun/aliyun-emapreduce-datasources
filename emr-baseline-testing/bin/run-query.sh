#!/bin/bash

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

set -e

base_dir=$(dirname $0)

source $base_dir/config.sh

if [ ! -d "$base_dir/../logs" ]; then
    mkdir "$base_dir/../logs"
fi

LOG=$base_dir/../logs/query-$1.log

echo "Run query $1 ..." | tee -a $LOG 2>&1

if [ "$TESTING_ENABLE"x = "true"x ]; then
    spark-sql -f $base_dir/../queries/q$1.sql --name q$1.sql --master yarn-client --num-executors ${NUM_EXECUTORS} --executor-cores ${EXECUTOR_CORES} --executor-memory ${EXECUTOR_MEMORY} --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} --hivevar CHECKPOINT_ROOT=${CHECKPOINT_ROOT} --hivevar TESTING_ENABLE=${TESTING_ENABLE} --hivevar TESTING_TIMEOUT_MS=${TESTING_TIMEOUT_MS}
else
    beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../queries/q$1.sql --master yarn --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} --hivevar CHECKPOINT_ROOT=${CHECKPOINT_ROOT} --hivevar TESTING_ENABLE=${TESTING_ENABLE} --hivevar TESTING_TIMEOUT_MS=${TESTING_TIMEOUT_MS} & >> $LOG 2>&1
fi