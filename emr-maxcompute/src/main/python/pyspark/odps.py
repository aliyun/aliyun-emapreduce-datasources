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

import os
from pyspark.rdd import RDD
from pyspark.serializers import PickleSerializer, BatchedSerializer
from tempfile import NamedTemporaryFile
from datetime import datetime
from datetime import date

from py4j.protocol import Py4JJavaError

__all__ = ["OdpsOps"]

DATE_FORMAT = "%Y-%m-%d"

def _date_to_string(d):
    if type(d) is date:
        return d.strftime(DATE_FORMAT)
    else:
        return d

def _string_to_date(s):
    try:
        d = datetime.strptime(s, DATE_FORMAT).date()
        return d
    except:
        return s

class OdpsOps():
    def __init__(self, sc, accessId, accessKey, odpsUrl, tunnelUrl):
        self._sc = sc
        try:
            helperClass = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.aliyun.odps.PythonOdpsAPIHelper")
            helper = helperClass.newInstance()
            self._api = helper.createPythonOdpsAPI(sc._jsc, accessId, accessKey, odpsUrl, tunnelUrl)
        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                OdpsOps._printErrorMsg()
            raise e
        self._gateway = sc._gateway

    def readPartitionTable(self, project, table, partition, numPartitions, cols=[], bytesCols=[], batchSize=1):
        jcols = self._to_java_array(cols)
        jbytesCols = self._to_java_array(bytesCols)
        jrdd = self._api.readTable(project, table, partition, jcols, jbytesCols, batchSize, numPartitions)
        return RDD(jrdd, self._sc, PickleSerializer())

    def readNonPartitionTable(self, project, table, numPartitions, cols=[], bytesCols=[], batchSize=1):
        jcols = self._to_java_array(cols)
        jbytesCols = self._to_java_array(bytesCols)
        jrdd = self._api.readTable(project, table, jcols, jbytesCols, batchSize, numPartitions)
        return RDD(jrdd, self._sc, PickleSerializer())

    def saveToPartitionTable(self, project, table, partition, rdd, cols=[], bytesCols=[], isCreatePt=False, isOverWrite=False):
        pickledRDD = rdd._pickled()
        jcols = self._to_java_array(cols)
        jbytesCols = self._to_java_array(bytesCols)
        batched = isinstance(pickledRDD._jrdd_deserializer, BatchedSerializer)
        self._api.saveToTable(project, table, partition, pickledRDD._jrdd, jcols, jbytesCols, batched, isCreatePt, isOverWrite)

    def saveToNonPartitionTable(self, project, table, rdd, cols=[], bytesCols=[]):
        pickledRDD = rdd._pickled()
        jcols = self._to_java_array(cols)
        jbytesCols = self._to_java_array(bytesCols)
        batched = isinstance(pickledRDD._jrdd_deserializer, BatchedSerializer)
        self._api.saveToTable(project, table, pickledRDD._jrdd, jcols, jbytesCols, batched)

    def getTableSchema(self, project, table, isPartition):
        bytesInJava  = self._api.getTableSchema(project, table, isPartition).iterator()
        li = list(self._collect_iterator_through_file(bytesInJava))
        res = []
        for i in range(0, len(li), 2):
            res.append((li[i], li[i+1]))
        return res

    def getColumnByName(self, project, table, name):
        bytesInJava = self._api.getColumnByName(project, table, name).iterator()
        ret = list(self._collect_iterator_through_file(bytesInJava))
        tuple = (ret[0], ret[1])
        return tuple

    def getColumnByIdx(self, project, table, idx):
        bytesInJava = self._api.getColumnByIdx(project, table, idx).iterator()
        ret = list(self._collect_iterator_through_file(bytesInJava))
        tuple = (ret[0], ret[1])
        return tuple

    def _to_java_array(self, int_arr):
        jarray = self._gateway.new_array(self._gateway.jvm.int, len(int_arr))
        for idx, val in enumerate(int_arr):
            jarray[idx] = val
        return jarray

    def _collect_iterator_through_file(self, iterator):
        # Transferring lots of data through Py4J can be slow because
        # socket.readline() is inefficient.  Instead, we'll dump the data to a
        # file and read it back.
        tempFile = NamedTemporaryFile(delete=False, dir=self._sc._temp_dir)
        tempFile.close()
        self._api.writeToFile(iterator, tempFile.name)
        # Read the data into Python and deserialize it:
        with open(tempFile.name, 'rb') as tempFile:
            for item in PickleSerializer().load_stream(tempFile):
                yield item
        os.unlink(tempFile.name)

    @staticmethod
    def _printErrorMsg():
        print("""
________________________________________________________________________________________________

  E-MapReduce SDK's libraries not found in class path. Try one of the following.

  1. Include the 'emr-maxcompute_2.11' library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages com.aliyun.emr:emr-maxcompute_2.11:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = com.aliyun.emr, Artifact Id = emr-maxcompute_2.11, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <emr-maxcompute_2.11-%s.jar> ...

________________________________________________________________________________________________

""" % ('2.0.0-SNAPSHOT', '2.0.0-SNAPSHOT', '2.0.0-SNAPSHOT'))
