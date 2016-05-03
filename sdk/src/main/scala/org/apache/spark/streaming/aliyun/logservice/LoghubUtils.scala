/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.aliyun.logservice

import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Various utility classes for working with Aliyun LogService.
 */
object LoghubUtils {
  /**
   *{{{
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     endpoint,
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      new LoghubInputDStream(
        ssc,
        logServiceProject,
        logStoreName,
        loghubConsumerGroupName,
        appId,
        loghubEndpoint,
        accessKeyId,
        accessKeySecret,
        storageLevel)
    }
  }

  /**
   * Create loghub [[DStream]].
   *{{{
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val numReceivers = 2
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     endpoint,
   *     numReceivers,
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      ssc.union(Array.tabulate(numReceivers)(e => e).map(t =>
        new LoghubInputDStream(
          ssc,
          logServiceProject,
          logStoreName,
          loghubConsumerGroupName,
          appId,
          loghubEndpoint,
          accessKeyId,
          accessKeySecret,
          storageLevel)
      ))
    }
  }

  @Experimental
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName, loghubConsumerGroupName, loghubEndpoint, numReceivers,
      accessKeyId, accessKeySecret, storageLevel)
  }
}
