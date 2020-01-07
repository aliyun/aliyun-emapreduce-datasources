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

import scala.collection.JavaConverters._

import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.batch.aliyun.logservice.LoghubBatchRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaDStream, JavaInputDStream, JavaReceiverInputDStream, JavaStreamingContext}
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
   * @param loghubConsumerGroupName The group name of loghub consumer.
    *       All consumer process which has the same group name will consumer
    *       specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
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
   *{{{
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer.
   *       All consumer process which has the same group name will consumer
   *       specific logStore together.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
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
        null,
        null,
        null,
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
   * @param loghubConsumerGroupName The group name of loghub consumer.
   *        All consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
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

  /**
   * Create loghub [[DStream]].
   *{{{
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
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
   *     numReceivers,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer.
   *        All consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param numReceivers The number of receivers.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      numReceivers: Int,
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
          null,
          null,
          null,
          storageLevel)
      ))
    }
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *        `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  // scalastyle:off
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): ReceiverInputDStream[Array[Byte]] = {
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
        storageLevel,
        cursorPosition,
        mLoghubCursorStartTime,
        forceSpecial)
    }
  }
  // scalastyle:on

  /**
   * Create loghub [[DStream]].
   *
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *        `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      new LoghubInputDStream(
        ssc,
        logServiceProject,
        logStoreName,
        loghubConsumerGroupName,
        appId,
        null,
        null,
        null,
        storageLevel,
        cursorPosition,
        mLoghubCursorStartTime,
        forceSpecial)
    }
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *                     `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  // scalastyle:off
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): DStream[Array[Byte]] = {
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
          storageLevel,
          cursorPosition,
          mLoghubCursorStartTime,
          forceSpecial)
      ))
    }
  }
  // scalastyle:on

  /**
   * Create loghub [[DStream]].
   *
   * @param ssc StreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param numReceivers The number of receivers.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *                     `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      numReceivers: Int,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): DStream[Array[Byte]] = {
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
          null,
          null,
          null,
          storageLevel,
          cursorPosition,
          mLoghubCursorStartTime,
          forceSpecial)
      ))
    }
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, loghubEndpoint, accessKeyId, accessKeySecret,
      storageLevel)
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, storageLevel)
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
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
    createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, loghubEndpoint, numReceivers, accessKeyId,
      accessKeySecret, storageLevel)
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param numReceivers The number of receivers.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      numReceivers: Int,
      storageLevel: StorageLevel): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, numReceivers, storageLevel)
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *                     `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  // scalastyle:off
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, loghubEndpoint, accessKeyId, accessKeySecret,
      storageLevel, cursorPosition, mLoghubCursorStartTime, forceSpecial)
  }
  // scalastyle:on

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *                     `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, storageLevel, cursorPosition, mLoghubCursorStartTime,
      forceSpecial)
  }

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *                     `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  // scalastyle:off
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
      loghubEndpoint, numReceivers, accessKeyId, accessKeySecret, storageLevel,
      cursorPosition, mLoghubCursorStartTime, forceSpecial)
  }
  // scalastyle:on

  /**
   * Create loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param logServiceProject The name of `LogService` project.
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param numReceivers The number of receivers.
   * @param storageLevel Storage level to use for storing the received objects.
   * @param cursorPosition Set user defined cursor type.
   * @param mLoghubCursorStartTime Set user defined cursor position (Unix Timestamp).
   * @param forceSpecial Whether to force to set consume position as the
   *                     `mLoghubCursorStartTime`.
   * @return
   */
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      numReceivers: Int,
      storageLevel: StorageLevel,
      cursorPosition: LogHubCursorPosition,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    createStream(jssc.ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
      numReceivers, storageLevel, cursorPosition, mLoghubCursorStartTime, forceSpecial)
  }

  /**
   * Create direct loghub [[DStream]].
   *
   * @param ssc StreamingContext.
   * @param project The name of `LogService` project.
   * @param logStore The name of logStore.
   * @param mConsumerGroup The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of loghub.
   * @param zkParams Zookeeper parameters.
   * @param mode Set user defined cursor type.
   * @return
   */
  @Experimental
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createDirectStream(
      ssc: StreamingContext,
      project: String,
      logStore: String,
      mConsumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      zkParams: Map[String, String],
      mode: LogHubCursorPosition): DStream[String] = {
    createDirectStream(ssc, project, logStore, mConsumerGroup, accessKeyId,
      accessKeySecret, endpoint, zkParams, mode, -1L)
  }

  /**
   * Create direct loghub [[DStream]].
   *
   * Set `cursorStartTime` a valid value when using LogHubCursorPosition.SPECIAL_TIMER_CURSOR mode
   * at the first time with current `mConsumerGroup`
   *
   * @param ssc StreamingContext.
   * @param project The name of `LogService` project.
   * @param logStore The name of logStore.
   * @param mConsumerGroup The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of loghub.
   * @param zkParams Zookeeper parameters.
   * @param mode Set user defined cursor type.
   * @param cursorStartTime Set user defined cursor position (Unix Timestamp).
   * @return
   */
  @Experimental
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createDirectStream(
      ssc: StreamingContext,
      project: String,
      logStore: String,
      mConsumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      zkParams: Map[String, String],
      mode: LogHubCursorPosition,
      cursorStartTime: Long): DStream[String] = {
    new DirectLoghubInputDStream(ssc, project, logStore, mConsumerGroup, accessKeyId,
      accessKeySecret, endpoint, zkParams, mode, cursorStartTime)
  }

  /**
   * Create direct loghub [[DStream]].
   *
   * @param jssc JavaStreamingContext.
   * @param project The name of `LogService` project.
   * @param logStore The name of logStore.
   * @param mConsumerGroup The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of loghub.
   * @param zkParams Zookeeper parameters.
   * @param mode Set user defined cursor type.
   * @return
   */
  @Experimental
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createDirectStream(
      jssc: JavaStreamingContext,
      project: String,
      logStore: String,
      mConsumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      zkParams: java.util.HashMap[String, String],
      mode: LogHubCursorPosition): JavaInputDStream[String] = {
    createDirectStream(jssc, project, logStore, mConsumerGroup, accessKeyId,
      accessKeySecret, endpoint, zkParams, mode, -1L)
  }

  /**
   * Create direct loghub [[DStream]].
   *
   * Set `cursorStartTime` a valid value when using LogHubCursorPosition.SPECIAL_TIMER_CURSOR mode
   * at the first time with current `mConsumerGroup`
   *
   * @param jssc StreamingContext.
   * @param project The name of `LogService` project.
   * @param logStore The name of logStore.
   * @param mConsumerGroup The group name of loghub consumer. All
   *        consumer process which has the same group name will consumer
   *        specific logStore together.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of loghub.
   * @param zkParams Zookeeper parameters.
   * @param mode Set user defined cursor type.
   * @param cursorStartTime Set user defined cursor position (Unix Timestamp).
   * @return
   */
  @Experimental
  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.readStream.format("loghub")' instead.""", "1.8.0")
  def createDirectStream(
      jssc: JavaStreamingContext,
      project: String,
      logStore: String,
      mConsumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      zkParams: java.util.HashMap[String, String],
      mode: LogHubCursorPosition,
      cursorStartTime: Long): JavaInputDStream[String] = {
    new JavaInputDStream(new DirectLoghubInputDStream(jssc.ssc, project, logStore,
      mConsumerGroup, accessKeyId, accessKeySecret, endpoint, zkParams.asScala.toMap,
      mode, cursorStartTime))
  }

  @deprecated("""This method has been deprecated and will be removed in a future release. """ +
    """Please use 'spark.read.format("loghub")' instead.""", "1.8.0")
  def createRDD(
      sc: SparkContext,
      project: String,
      logStore: String,
      accessId: String,
      accessKey: String,
      endpoint: String,
      startTime: Long,
      endTime: Long = -1,
      parallelismInShard: Int = 1) : RDD[String] = {
    new LoghubBatchRDD(sc, project, logStore, accessId, accessKey, endpoint, startTime,
      endTime = endTime, parallelismInShard = parallelismInShard)
  }
}

class LoghubUtilsHelper {

  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    LoghubUtils.createStream(jssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, loghubEndpoint, accessKeyId, accessKeySecret,
      storageLevel)
  }

  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    LoghubUtils.createStream(jssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, storageLevel)
  }

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
    LoghubUtils.createStream(jssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, loghubEndpoint, numReceivers, accessKeyId,
      accessKeySecret, storageLevel)
  }

  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      numReceivers: Int,
      storageLevel: StorageLevel): JavaDStream[Array[Byte]] = {
    LoghubUtils.createStream(jssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, numReceivers, storageLevel)
  }

  // scalastyle:off
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      cursorPosition: String,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    val cursor = cursorPosition match {
      case "BEGIN_CURSOR" => LogHubCursorPosition.BEGIN_CURSOR
      case "END_CURSOR" => LogHubCursorPosition.END_CURSOR
      case "SPECIAL_TIMER_CURSOR" => LogHubCursorPosition.SPECIAL_TIMER_CURSOR
      case e: String => throw new IllegalArgumentException(s"Unknown LogHubCursorPosition $e")
    }
    LoghubUtils.createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, loghubEndpoint, accessKeyId, accessKeySecret,
      storageLevel, cursor, mLoghubCursorStartTime, forceSpecial)
  }
  // scalastyle:on

  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      storageLevel: StorageLevel,
      cursorPosition: String,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    val cursor = cursorPosition match {
      case "BEGIN_CURSOR" => LogHubCursorPosition.BEGIN_CURSOR
      case "END_CURSOR" => LogHubCursorPosition.END_CURSOR
      case "SPECIAL_TIMER_CURSOR" => LogHubCursorPosition.SPECIAL_TIMER_CURSOR
      case e: String => throw new IllegalArgumentException(s"Unknown LogHubCursorPosition $e")
    }
    LoghubUtils.createStream(jssc.ssc, logServiceProject, logStoreName,
      loghubConsumerGroupName, storageLevel, cursor, mLoghubCursorStartTime,
      forceSpecial)
  }

  // scalastyle:off
  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      cursorPosition: String,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    val cursor = cursorPosition match {
      case "BEGIN_CURSOR" => LogHubCursorPosition.BEGIN_CURSOR
      case "END_CURSOR" => LogHubCursorPosition.END_CURSOR
      case "SPECIAL_TIMER_CURSOR" => LogHubCursorPosition.SPECIAL_TIMER_CURSOR
      case e: String => throw new IllegalArgumentException(s"Unknown LogHubCursorPosition $e")
    }
    LoghubUtils.createStream(jssc.ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
      loghubEndpoint, numReceivers, accessKeyId, accessKeySecret, storageLevel,
      cursor, mLoghubCursorStartTime, forceSpecial)
  }
  // scalastyle:on

  def createStream(
      jssc: JavaStreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      numReceivers: Int,
      storageLevel: StorageLevel,
      cursorPosition: String,
      mLoghubCursorStartTime: Int,
      forceSpecial: Boolean): JavaDStream[Array[Byte]] = {
    val cursor = cursorPosition match {
      case "BEGIN_CURSOR" => LogHubCursorPosition.BEGIN_CURSOR
      case "END_CURSOR" => LogHubCursorPosition.END_CURSOR
      case "SPECIAL_TIMER_CURSOR" => LogHubCursorPosition.SPECIAL_TIMER_CURSOR
      case e: String => throw new IllegalArgumentException(s"Unknown LogHubCursorPosition $e")
    }
    LoghubUtils.createStream(jssc.ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
      numReceivers, storageLevel, cursor, mLoghubCursorStartTime, forceSpecial)
  }

  @Experimental
  def createDirectStream(
      jssc: JavaStreamingContext,
      project: String,
      logStore: String,
      mConsumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      zkParams: java.util.HashMap[String, String],
      cursorPositionMode: String): JavaInputDStream[String] = {
    createDirectStream(jssc, project, logStore, mConsumerGroup, accessKeyId,
      accessKeySecret, endpoint, zkParams, cursorPositionMode, -1L)
  }

  @Experimental
  def createDirectStream(
      jssc: JavaStreamingContext,
      project: String,
      logStore: String,
      mConsumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      zkParams: java.util.HashMap[String, String],
      cursorPositionMode: String,
      cursorStartTime: Long): JavaInputDStream[String] = {
    val cursorMode = cursorPositionMode match {
      case "BEGIN_CURSOR" => LogHubCursorPosition.BEGIN_CURSOR
      case "END_CURSOR" => LogHubCursorPosition.END_CURSOR
      case "SPECIAL_TIMER_CURSOR" => LogHubCursorPosition.SPECIAL_TIMER_CURSOR
      case e: String => throw new IllegalArgumentException(s"Unknown LogHubCursorPosition $e")
    }
    new JavaInputDStream(new DirectLoghubInputDStream(jssc.ssc, project, logStore, mConsumerGroup,
      accessKeyId, accessKeySecret, endpoint, zkParams.asScala.toMap, cursorMode, cursorStartTime))
  }

  def createRDD(
      jsc: JavaSparkContext,
      project: String,
      logStore: String,
      accessId: String,
      accessKey: String,
      endpoint: String,
      startTime: Long,
      endTime: Long) : JavaRDD[String] = {
    LoghubUtils.createRDD(jsc.sc, project, logStore, accessId, accessKey, endpoint, startTime,
      endTime)
  }

  def createRDD(
      jsc: JavaSparkContext,
      project: String,
      logStore: String,
      accessId: String,
      accessKey: String,
      endpoint: String,
      startTime: Long) : JavaRDD[String] = {
    LoghubUtils.createRDD(jsc.sc, project, logStore, accessId, accessKey, endpoint, startTime)
  }
}
