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
package org.apache.spark.streaming.aliyun.mns

import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.aliyun.mns.pulling.MnsPullingInputDStream
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * Various utility classes for working with Aliyun MNS.
 */
object MnsUtils {
  /**
    * {{{
    *    val queuename = "queueSample"
    *    val accessKeyId = "kj7aY*******UYx6"
    *    val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
    *    val endpoint = "http://184*********815.mns-test.aliyuncs.com:1234"
    *
    *    val conf = new SparkConf().setAppName("Test MNS")
    *    val ssc = new StreamingContext(conf, Milliseconds(2000))
    *    val mnsStream = MnsUtils.createPullingStreamAsBytes(ssc, queuename,
    *      accessKeyId, accessKeySecret, endpoint, StorageLevel.MEMORY_ONLY)
    * }}}
    * @param ssc StreamingContext.
    * @param queueName The name of MNS queue.
    * @param accessKeyId The Aliyun Access Key Id.
    * @param accessKeySecret The Aliyun Access Key Secret.
    * @param endpoint The endpoint of MNS service.
    * @param storageLevel Storage level to use for storing the received objects.
    * @return
    */
  def createPullingStreamAsBytes(
      ssc: StreamingContext,
      queueName: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("mns stream as bytes") {
      new MnsPullingInputDStream(ssc, queueName, accessKeyId, accessKeySecret,
        endpoint, storageLevel, false)
    }
  }

  @Experimental
  def createPullingStreamAsBytes(
      ssc: StreamingContext,
      queueName: String,
      endpoint: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("mns stream as bytes") {
      new MnsPullingInputDStream(ssc, queueName, null, null, endpoint,
        storageLevel, false)
    }
  }

  /**
   * {{{
   *    val queuename = "queueSample"
   *    val accessKeyId = "kj7aY*******UYx6"
   *    val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *    val endpoint = "http://184*********815.mns-test.aliyuncs.com:1234"
   *
   *    val conf = new SparkConf().setAppName("Test MNS")
   *    val ssc = new StreamingContext(conf, Milliseconds(2000))
   *    val mnsStream = MnsUtils.createPullingStreamAsRawBytes(ssc, queuename,
   *      accessKeyId, accessKeySecret, endpoint, StorageLevel.MEMORY_ONLY)
   * }}}
   * @param ssc StreamingContext.
   * @param queueName The name of MNS queue.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of MNS service.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  def createPullingStreamAsRawBytes(
      ssc: StreamingContext,
      queueName: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("mns stream as raw bytes") {
      new MnsPullingInputDStream(ssc, queueName, accessKeyId, accessKeySecret,
        endpoint, storageLevel, true)
    }
  }

  @Experimental
  def createPullingStreamAsRawBytes(
      ssc: StreamingContext,
      queueName: String,
      endpoint: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("mns stream as raw bytes") {
      new MnsPullingInputDStream(ssc, queueName, null, null, endpoint,
        storageLevel, true)
    }
  }

  /**
   * {{{
   *    String queuename = "queueSample";
   *    String accessKeyId = "kj7aY*******UYx6";
   *    String accessKeySecret = "AiNMAlxz*************1PxaPaL8t";
   *    String endpoint = "http://184*********815.mns-test.aliyuncs.com:1234";
   *
   *    JavaStreamingContext jssc = ...;
   *    JavaReceiverInputDStream<Byte[]> mnsStream = MnsUtils
   *      .createPullingStreamAsBytes(
   *        jssc,
   *        queuename,
   *        accesskeyId,
   *        accessKeySecret,
   *        endpoint,
   *        StorageLevel.MEMORY_AND_DISK_2);
   * }}}
   * @param jssc Java streamingContext object.
   * @param queueName The name of MNS queue.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of MNS service.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  def createPullingStreamAsBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createPullingStreamAsBytes(jssc.ssc, queueName, accessKeyId, accessKeySecret,
      endpoint, storageLevel)
  }

  @Experimental
  def createPullingStreamAsBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createPullingStreamAsBytes(jssc.ssc, queueName, null, null, endpoint,
      storageLevel)
  }

  /**
   * {{{
   *    String queuename = "queueSample";
   *    String accessKeyId = "kj7aY*******UYx6";
   *    String accessKeySecret = "AiNMAlxz*************1PxaPaL8t";
   *    String endpoint = "http://184*********815.mns-test.aliyuncs.com:1234";
   *
   *    JavaStreamingContext jssc = ...;
   *    JavaReceiverInputDStream<Byte[]> mnsStream = MnsUtils
   *      .createPullingStreamAsRawBytes(
   *        jssc,
   *        queuename,
   *        accesskeyId,
   *        accessKeySecret,
   *        endpoint,
   *        StorageLevel.MEMORY_AND_DISK_2);
   * }}}
   * @param jssc Java streamingContext object.
   * @param queueName The name of MNS queue.
   * @param accessKeyId The Aliyun Access Key Id.
   * @param accessKeySecret The Aliyun Access Key Secret.
   * @param endpoint The endpoint of MNS service.
   * @param storageLevel Storage level to use for storing the received objects.
   * @return
   */
  def createPullingStreamAsRawBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createPullingStreamAsRawBytes(jssc.ssc, queueName, accessKeyId,
      accessKeySecret, endpoint, storageLevel)
  }

  @Experimental
  def createPullingStreamAsRawBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createPullingStreamAsRawBytes(jssc.ssc, queueName, null, null, endpoint,
      storageLevel)
  }
}

class MnsUtilsHelper {

  def createPullingStreamAsBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    MnsUtils.createPullingStreamAsBytes(jssc.ssc, queueName, accessKeyId,
      accessKeySecret, endpoint, storageLevel)
  }

  @Experimental
  def createPullingStreamAsBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    MnsUtils.createPullingStreamAsBytes(jssc.ssc, queueName, null, null,
      endpoint, storageLevel)
  }

  @Experimental
  def createPullingStreamAsRawBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    MnsUtils.createPullingStreamAsRawBytes(jssc.ssc, queueName, accessKeyId,
      accessKeySecret, endpoint, storageLevel)
  }

  def createPullingStreamAsRawBytes(
      jssc: JavaStreamingContext,
      queueName: String,
      endpoint: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    MnsUtils.createPullingStreamAsRawBytes(jssc.ssc, queueName, null, null,
      endpoint, storageLevel)
  }
}
