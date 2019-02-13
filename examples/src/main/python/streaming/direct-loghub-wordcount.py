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

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from loghub import LoghubUtils

if __name__ == "__main__":
    if len(sys.argv) < 9:
        print >> sys.stderr, "Usage: spark-submit direct-loghub-wordcount.py logServiceProject logsStoreName " \
                             "logHubConsumerGroupName accessKeyId accessKeySecret loghubEndpoint" \
                             "zkAddr cursorPositionMode [cursorStartTime]"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectLoghubWordCount")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("hdfs:///tmp/spark/streaming")

    project = sys.argv[1]
    logstore = sys.argv[2]
    group = sys.argv[3]
    id = sys.argv[4]
    secret = sys.argv[5]
    endpoint = sys.argv[6]
    zkAddr = sys.argv[7]
    cursorPositionMode = sys.argv[8]

    zkParams = {}
    zkParams["zookeeper.connect"] = zkAddr
    zkParams["enable.auto.commit"] = "false"

    stream = LoghubUtils.createDirectStream(ssc, project, logstore, group,
                                      id, secret, endpoint, zkParams, cursorPositionMode)
    lines = stream.map(lambda x: x)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
