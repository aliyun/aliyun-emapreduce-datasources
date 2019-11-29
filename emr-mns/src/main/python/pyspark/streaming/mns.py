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

from py4j.protocol import Py4JJavaError
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import DStream
from pyspark.serializers import UTF8Deserializer

__all__ = ['MnsUtils']

class MnsUtils(object):

    @staticmethod
    def createPullingStreamAsBytes(ssc, queueName, accessKeyId, accessKeySecret,
                                   endpoint, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        :param ssc: StreamingContext object.
        :param queueName: The name of MNS queue.
        :param accessKeyId: Aliyun Access Key ID.
        :param accessKeySecret: Aliyun Access Key Secret.
        :param endpoint: The endpoint of MNS service.
        :param storageLevel: RDD storage level.
        :return: A DStream object.
        """
        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.mns.MnsUtilsHelper")
            helper = helperClass.newInstance()
            jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
            jstream = helper.createPullingStreamAsBytes(ssc._jssc, queueName, accessKeyId, accessKeySecret, endpoint, jlevel)

        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                MnsUtils._printErrorMsg()
            raise e
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def createPullingStreamAsRawBytes(ssc, queueName, accessKeyId, accessKeySecret,
                                      endpoint, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        :param ssc: StreamingContext object.
        :param queueName: The name of MNS queue.
        :param accessKeyId: Aliyun Access Key ID.
        :param accessKeySecret: Aliyun Access Key Secret.
        :param endpoint: The endpoint of MNS service.
        :param storageLevel: RDD storage level.
        :return: A DStream object.
        """
        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.mns.MnsUtilsHelper")
            helper = helperClass.newInstance()
            jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
            jstream = helper.createPullingStreamAsRawBytes(ssc._jssc, queueName, accessKeyId, accessKeySecret, endpoint, jlevel)

        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                MnsUtils._printErrorMsg()
            raise e
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def _printErrorMsg():
        print("""
________________________________________________________________________________________________

  E-MapReduce SDK's libraries not found in class path. Try one of the following.

  1. Include the 'emr-mns_2.11' library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages com.aliyun.emr:emr-mns_2.11:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = com.aliyun.emr, Artifact Id = emr-mns_2.11, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <emr-mns_2.11-%s.jar> ...

________________________________________________________________________________________________

""" % ('2.0.0-SNAPSHOT', '2.0.0-SNAPSHOT', '2.0.0-SNAPSHOT'))
