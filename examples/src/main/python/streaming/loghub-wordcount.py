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
    if len(sys.argv) != 8:
        print >> sys.stderr, "Usage: spark-submit loghub-wordcount.py logServiceProject logsStoreName " \
                             "logHubConsumerGroupName loghubEndpoint numReceiver accessKeyId accessKeySecret"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingLoghubWordCount")
    ssc = StreamingContext(sc, 2)

    logServiceProject = sys.argv[1]
    logsStoreName = sys.argv[2]
    logHubConsumerGroupName = sys.argv[3]
    loghubEndpoint = sys.argv[4]
    numReceiver = int(sys.argv[5])
    accessKeyId = sys.argv[6]
    accessKeySecret = sys.argv[7]

    stream = LoghubUtils.createStreams(ssc, logServiceProject, logsStoreName, logHubConsumerGroupName, loghubEndpoint,
                                       numReceiver, accessKeyId, accessKeySecret)
    lines = stream.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
