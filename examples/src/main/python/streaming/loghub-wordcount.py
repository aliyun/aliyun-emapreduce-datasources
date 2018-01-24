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

from pyspark.storagelevel import StorageLevel
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

    project = sys.argv[1]
    logstore = sys.argv[2]
    group = sys.argv[3]
    endpoint = sys.argv[4]
    num = int(sys.argv[5])
    id = sys.argv[6]
    secret = sys.argv[7]

    stream = LoghubUtils.createStream(ssc, project, logstore, group,
                                      numReceivers=num, loghubEndpoint=endpoint, accessKeyId=id, accessKeySecret=secret,
                                      cursorPosition=None, mLoghubCursorStartTime=None, forceSpecial=None,
                                      storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2)
    lines = stream.map(lambda x: x)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
