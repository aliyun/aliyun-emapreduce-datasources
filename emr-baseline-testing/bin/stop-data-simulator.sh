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

base_dir="$(cd "`dirname "$0"`/.."; pwd)"
cd $base_dir/jobs
jobs=`ls application*`
for job in $jobs
do
    yarn application --kill $job
    rm -f $job
done

uuids=`ls`
for uuid in $uuids
do
    PID=$(ps ax | grep SparkSubmit | grep "$uuid"| grep java | grep -v grep | awk '{print $1}')
    kill -9 $PID
    rm -f $uuid
done