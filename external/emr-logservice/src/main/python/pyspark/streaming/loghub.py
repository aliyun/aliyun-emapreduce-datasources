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

__all__ = ['LoghubUtils']

class LoghubUtils(object):

    @staticmethod
    def createStreams(ssc, logServiceProject, logStoreName, loghubConsumerGroupName, loghubEndpoint, numReceivers,
                      accessKeyId, accessKeySecret, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        :param ssc: StreamingContext object.
        :param logServiceProject: The name of `LogService` project.
        :param logStoreName: The name of logStore.
        :param loghubConsumerGroupName: The group name of loghub consumer. All consumer process which has the same group
                                       name will consumer specific logStore together.
        :param loghubEndpoint: The endpoint of loghub.
        :param numReceivers: The number of receivers.
        :param accessKeyId: Aliyun Access Key ID.
        :param accessKeySecret: Aliyun Access Key Secret.
        :param storageLevel: RDD storage level.
        :return: A DStream object.
        """
        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.logservice.LoghubUtilsHelper")
            helper = helperClass.newInstance()
            jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
            jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                          loghubConsumerGroupName, loghubEndpoint, numReceivers,
                                          accessKeyId, accessKeySecret, jlevel)

        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                LoghubUtils._printErrorMsg()
            raise e
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def createStream(ssc, logServiceProject, logStoreName, loghubConsumerGroupName, loghubEndpoint,
                      accessKeyId, accessKeySecret, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
        """
        :param ssc: StreamingContext object.
        :param logServiceProject: The name of `LogService` project.
        :param logStoreName: The name of logStore.
        :param loghubConsumerGroupName: The group name of loghub consumer. All consumer process which has the same group
                                       name will consumer specific logStore together.
        :param loghubEndpoint: The endpoint of loghub.
        :param numReceivers: The number of receivers.
        :param accessKeyId: Aliyun Access Key ID.
        :param accessKeySecret: Aliyun Access Key Secret.s
        :param storageLevel: RDD storage level.
        :return: A DStream object.
        """
        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.logservice.LoghubUtilsHelper")
            helper = helperClass.newInstance()
            jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
            jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                          loghubConsumerGroupName, loghubEndpoint,
                                          accessKeyId, accessKeySecret, jlevel)

        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                LoghubUtils._printErrorMsg()
            raise e
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def _printErrorMsg():
        print("""
________________________________________________________________________________________________

  E-MapReduce SDK's libraries not found in class path. Try one of the following.

  1. Include the 'emr-logservice_2.11' library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages com.aliyun.emr:emr-logservice_2.11:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = com.aliyun.emr, Artifact Id = emr-logservice_2.11, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <emr-logservice_2.11-%s.jar> ...

________________________________________________________________________________________________

""" % ('1.4.1', '1.4.1', '1.4.1'))