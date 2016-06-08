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

from odps import OdpsOps
from pyspark import SparkContext

if __name__ == "__main__":

    if len(sys.argv) != 7:
        print >> sys.stderr, "Usage: spark-submit odps-sample.py accessKeyId accessKeySecret project table " \
                             "partition numPartitions"
        exit(-1)

    accessKeyId = sys.argv[1]
    accessKeySecret = sys.argv[2]
    odpsUrl = "http://odps-ext.aliyun-inc.com/api"
    tunnelUrl = "http://dt-ext.odps.aliyun-inc.com"
    project = sys.argv[3]
    table = sys.argv[4]
    partition = sys.argv[5]
    numPartitions = sys.argv[6]

    sc = SparkContext(appName="PySpark Odps Sample")

    odpsOps = OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)

    print "pScheme"
    pSchema = odpsOps.getTableSchema(project, table, True)
    for col in pSchema:
        print col

    print "scheme"
    schema = odpsOps.getTableSchema(project, table, False)
    for col in schema:
        print col

    print "ColumnByIdx"
    col1 =odpsOps.getColumnByIdx(project, table, 1)
    print col1

    data = sc.parallelize([[1, 1.5, False, "2014-06-11", "row 1"],
                           [2, 1.5, True, "2014-06-10", "row 2"]], 2)
    odpsOps.saveToPartitionTable(project, table, partition, data, isCreatePt=True, isOverWrite=False)

    nump = int(numPartitions)
    rdd = odpsOps.readPartitionTable(project, table, partition, nump, batchSize=1)
    rows = rdd.collect()
    for row in rows:
        print "row: ",
        for col in row:
            print col, type(col),
        print ""

    print "read specific columns"
    rdd2 = odpsOps.readPartitionTable(project, table, partition, nump, cols=[1, 2])
    rows2 = rdd2.collect()
    for row in rows2:
        print "row: ",
        for col in row:
            print col, type(col),
        print ""
