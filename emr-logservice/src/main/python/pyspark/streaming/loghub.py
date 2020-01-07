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
from pyspark import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming import DStream
from pyspark.serializers import UTF8Deserializer

__all__ = ['LoghubUtils']

class LoghubUtils(object):

    @staticmethod
    def createStream(ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
                      numReceivers=None, loghubEndpoint=None, accessKeyId=None, accessKeySecret=None,
                      cursorPosition=None, mLoghubCursorStartTime=None, forceSpecial=None,
                      storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2):
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
        :param cursorPosition: Set user defined cursor type.
        :param mLoghubCursorStartTime: Set user defined cursor position (Unix Timestamp).
        :param forceSpecial: Whether to force to set consume position as the `mLoghubCursorStartTime`.
        :param storageLevel: RDD storage level.
        :return: A DStream object.
        """
        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.logservice.LoghubUtilsHelper")
            helper = helperClass.newInstance()
            jlevel = ssc._sc._getJavaStorageLevel(storageLevel)
            if numReceivers is None:
                if loghubEndpoint and accessKeySecret and accessKeyId:
                    if cursorPosition and mLoghubCursorStartTime and forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                            loghubConsumerGroupName, loghubEndpoint,
                                            accessKeyId, accessKeySecret, jlevel, cursorPosition,
                                            mLoghubCursorStartTime, forceSpecial)
                    elif not cursorPosition and not mLoghubCursorStartTime and not forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                              loghubConsumerGroupName, loghubEndpoint,
                                              accessKeyId, accessKeySecret, jlevel)
                    else:
                        raise TypeError("Should provide cursorPosition, mLoghubCursorStartTime, forceSpecial or not at the same time")
                elif not loghubEndpoint and not accessKeySecret and not accessKeyId:
                    if cursorPosition and mLoghubCursorStartTime and forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                                      loghubConsumerGroupName, jlevel, cursorPosition,
                                                      mLoghubCursorStartTime, forceSpecial)
                    elif not cursorPosition and not mLoghubCursorStartTime and not forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                                      loghubConsumerGroupName, jlevel)
                    else:
                        raise TypeError("Should provide cursorPosition, mLoghubCursorStartTime, forceSpecial or not at the same time")
                else:
                    raise TypeError("Should provide loghubEndpoint, accessKeySecret and accessKeyId or not at the same time")
            else:
                if loghubEndpoint and accessKeySecret and accessKeyId:
                    if cursorPosition and mLoghubCursorStartTime and forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                                      loghubConsumerGroupName, loghubEndpoint, numReceivers,
                                                      accessKeyId, accessKeySecret, jlevel, cursorPosition,
                                                      mLoghubCursorStartTime, forceSpecial)
                    elif not cursorPosition and not mLoghubCursorStartTime and not forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                                      loghubConsumerGroupName, loghubEndpoint, numReceivers,
                                                      accessKeyId, accessKeySecret, jlevel)
                    else:
                        raise TypeError("Should provide cursorPosition, mLoghubCursorStartTime, forceSpecial or not at the same time")
                elif not loghubEndpoint and not accessKeySecret and not accessKeyId:
                    if cursorPosition and mLoghubCursorStartTime and forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                                      loghubConsumerGroupName, numReceivers, jlevel,
                                                      cursorPosition, mLoghubCursorStartTime, forceSpecial)
                    elif not cursorPosition and not mLoghubCursorStartTime and not forceSpecial:
                        jstream = helper.createStream(ssc._jssc, logServiceProject, logStoreName,
                                                      loghubConsumerGroupName, numReceivers, jlevel)
                    else:
                        raise TypeError("Should provide cursorPosition, mLoghubCursorStartTime, forceSpecial or not at the same time")
                else:
                    raise TypeError("Should provide loghubEndpoint, accessKeySecret and accessKeyId or not at the same time")


        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                LoghubUtils._printErrorMsg()
            raise e
        return DStream(jstream, ssc, UTF8Deserializer())

    @staticmethod
    def createRDD(sc, logServiceProject, logStoreName, accessKeyId, accessKeySecret,
                  loghubEndpoint, startTime, endTime=None):
        """
        :param sc: RDD object.
        :param logServiceProject: The name of `LogService` project.
        :param logStoreName: The name of logStore.
        :param accessKeyId: Aliyun Access Key ID.
        :param accessKeySecret: Aliyun Access Key Secret.
        :param loghubEndpoint: The endpoint of loghub.
        :param startTime: Set user defined startTime (Unix Timestamp).
        :param endTime: Set user defined endTime (Unix Timestamp).
        :return: A RDD object.
        """
        try:
            helperClass = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.logservice.LoghubUtilsHelper")
            helper = helperClass.newInstance()
            if endTime:
                jrdd = helper.createRDD(sc._jsc, logServiceProject, logStoreName, accessKeyId, accessKeySecret,
                                        loghubEndpoint, startTime, endTime)
            else:
                jrdd = helper.createRDD(sc._jsc, logServiceProject, logStoreName, accessKeyId, accessKeySecret,
                                        loghubEndpoint, startTime)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                LoghubUtils._printErrorMsg()
            raise e

        return RDD(jrdd, sc, UTF8Deserializer())

    @staticmethod
    def createDirectStream(ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
                           accessKeyId, accessKeySecret, loghubEndpoint,
                           zkParams, cursorPositionMode, cursorStartTime=-1):
        """
        :param ssc: StreamingContext object.
        :param logServiceProject: The name of `LogService` project.
        :param logStoreName: The name of logStore.
        :param loghubConsumerGroupName: The group name of loghub consumer. All consumer process which has the same group
                                       name will consumer specific logStore together.
        :param accessKeyId: Aliyun Access Key ID.
        :param accessKeySecret: Aliyun Access Key Secret.
        :param loghubEndpoint: The endpoint of loghub.
        :param zkParams: Zookeeper properties.
        :param cursorPositionMode: Set user defined cursor mode.
        :param cursorStartTime: Set user defined cursor position (Unix Timestamp), -1 default.
        :return: A Direct api DStream object.
        """
        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.aliyun.logservice.LoghubUtilsHelper")
            helper = helperClass.newInstance()
            jstream = helper.createDirectStream(ssc._jssc, logServiceProject, logStoreName, loghubConsumerGroupName,
                                                accessKeyId, accessKeySecret, loghubEndpoint, zkParams,
                                                cursorPositionMode, cursorStartTime)
        except Py4JJavaError as e:
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

""" % ('2.0.0-SNAPSHOT', '2.0.0-SNAPSHOT', '2.0.0-SNAPSHOT'))
