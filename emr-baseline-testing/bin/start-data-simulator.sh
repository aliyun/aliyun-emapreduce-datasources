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

if [ $# -lt 1 ];
then
	echo "Usage: ./bin/start-data-simulator.sh -database database -tables tables -warehouse warehouseLocation
	            -bootstrapServers bootstrapServers -schemaRegistryUrl schemaRegistryUrl -throughput throughput [-unbound]"
	exit 1
fi
base_dir="$(cd "`dirname "$0"`/.."; pwd)"

if [ -z "${SPARK_HOME}" ]; then
  source "$(dirname "$0")"/find-spark-home
fi

UNBOUND="false"
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -database)
      DATABASE=$2
      shift 2
      ;;
    -tables)
      TABLES=$2
      shift 2
      ;;
    -warehouse)
      WAREHOUSE_LOCATION=$2
      shift 2
      ;;
    -bootstrapServers)
      BOOTSTRAP_SERVERS=$2
      shift 2
      ;;
    -schemaRegistryUrl)
      SCHEMA_REGISTRY_URL=$2
      shift 2
      ;;
    -throughput)
      THROUGHPUT=$2
      shift 2
      ;;
    -unbound)
      UNBOUND="true"
      shift 1
      ;;
    *)
      break
      ;;
  esac
done

if [ ! -d "$base_dir/jobs" ]; then
    mkdir "$base_dir/jobs"
fi

JOB_JAR=`ls $base_dir/lib/emr-baseline-testing*.jar`
TABLES=${TABLES//,/ }
for table in $TABLES
do
    topic=${table//_/}
    schema=${table//_/}
    uuid=`cat /proc/sys/kernel/random/uuid`
    echo "Replicating table($table) to kafka topic($topic)"
    nohup "${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.emr.baseline.testing.ReplicateHiveTableToKafka --master yarn-client --driver-memory 512m --num-executors 2 --executor-cores 2 --executor-memory 1g ${JOB_JAR} $DATABASE $table ${WAREHOUSE_LOCATION} $topic $schema ${BOOTSTRAP_SERVERS} ${SCHEMA_REGISTRY_URL} $uuid $THROUGHPUT $UNBOUND >/dev/null 2>&1 &
    while true; do
        yarn application --list >/tmp/$uuid 2>&1
        jobid=`cat /tmp/$uuid | grep "$uuid" | cut -c1-30`
        if [ -n "$jobid" ]; then
            break
        fi
    done
    touch "$base_dir/jobs/$jobid"
    touch "$base_dir/jobs/$uuid"
done