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

LOG=$base_dir/../logs/load.log

echo "Begin to initialize result topic schema ..." | tee -a $LOG 2>&1
schemas=`ls $base_dir/../schemas`
for schema_file in ${schemas}
do
    s=(${schema_file//./ })
    topic=${s[0]}
    python $base_dir/schema_register.py ${SCHEMA_REGISTRY_URL} ${topic} $base_dir/../schemas/${schema_file}
done
echo "Initialize result topic schema completed!" | tee -a $LOG 2>&1

echo "Begin to create kafka stream tables ..." | tee -a $LOG 2>&1
{
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_store_sales.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_store_returns.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_catalog_sales.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_catalog_returns.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_web_sales.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_web_returns.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
  beeline -u jdbc:hive2://localhost:${PORT}/${DB} -f $base_dir/../tables/kafka_inventory.sql --hivevar DB=${DB} --hivevar BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS} --hivevar SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL} &
} >> $LOG 2>&1
echo "Create kafka stream tables complete!" | tee -a $LOG 2>&1