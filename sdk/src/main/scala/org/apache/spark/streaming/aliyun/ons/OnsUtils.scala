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
package org.apache.spark.streaming.aliyun.ons

import com.aliyun.openservices.ons.api.Message
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * Various utility classes for working with Aliyun ONS.
 */
object OnsUtils {
  /**
   * Create an input stream that pulls message from a Aliyun ONS stream.
   * {{{
   *    val ssc: StreamingSparkContext = ...
   *    val cid = "ConsumerID"
   *    val topic = "sample-topic"
   *    val subExpression = "*"
   *    val accessKeyId = "kj7aY*******UYx6"
   *    val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *
   *    def func: Message => Array[Byte] = msg => msg.getBody
   *
   *    val onsStream = OnsUtils.createStream(
   *     ssc,
   *     cId,
   *     topic,
   *     subExpression,
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK_2,
   *     func)
   *
   *     onsStream.foreachRDD(rdd => {
   *       ...
   *     })
   * }}}
   * @param ssc StreamingContext object.
   * @param consumerId Name of a set of consumers.
   * @param topic Which topic to subscribe.
   * @param tags Which tag to subscribe.
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param func Extract information from ONS message.
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      consumerId: String,
      topic: String,
      tags: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      func: Message => Array[Byte]): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("ons stream") {
      new OnsInputDStream(ssc, consumerId, topic, tags, accessKeyId, accessKeySecret, storageLevel, func)
    }
  }

  /**
   * Create an union input stream that pulls message from a Aliyun ONS stream.
   * {{{
   *    val ssc: StreamingSparkContext = ...
   *    val consumerIdTopicTags = Array(("ConsumerID1", "sample-topic1", "*"))
   *    val accessKeyId = "kj7aY*******UYx6"
   *    val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *
   *    def func: Message => Array[Byte] = msg => msg.getBody
   *
   *    val onsStream = OnsUtils.createStream(
   *     ssc,
   *
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK_2,
   *     func)
   *
   *     onsStream.foreachRDD(rdd => {
   *       ...
   *     })
   * }}}
   * @param ssc StreamingContext object.
   * @param consumerIdTopicTags Trituple(consumerId, topic, tag).
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param func Extract information from ONS message.
   * @return
   */
  @Experimental
  def createStreams(
      ssc: StreamingContext,
      consumerIdTopicTags: Array[(String, String, String)],
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      func: Message => Array[Byte]): DStream[Array[Byte]] = {
    val invalidTuples1 = consumerIdTopicTags.groupBy(e => (e._1, e._2)).filter(e => e._2.length > 1)
    val invalidTuples2 = consumerIdTopicTags.map(e => (e._1, e._2)).groupBy(e => e._1).filter(e => e._2.length > 1)
    if (invalidTuples1.size > 1 || invalidTuples2.size > 1) {
      throw new RuntimeException("Inconsistent consumer subscription.")
    } else {
      ssc.union(consumerIdTopicTags.map({
        case (consumerId, topic, tags) =>
          createStream(ssc, consumerId, topic, tags, accessKeyId, accessKeySecret, storageLevel, func)
      }))
    }
  }

  /**
   * Create an input stream that pulls message from a Aliyun ONS stream.
   * {{{
   *    JavaStreamingContext jssc = ...;
   *    String cid = "ConsumerID";
   *    String topic = "sample-topic";
   *    String subExpression = "*";
   *    String accessKeyId = "kj7aY*******UYx6";
   *    String accessKeySecret = "AiNMAlxz*************1PxaPaL8t";
   *
   *    static class ReadMessage implements Function<Message, Byte[]> {
   *        @Override
   *        public Byte[] call(Message msg) {
   *            return msg.getBody;
   *    }
   *
   *    JavaReceiverInputDStream<Byte[]> onsStream = OnsUtils.createStream(
   *        ssc,
   *        cId,
   *        topic,
   *        subExpression,
   *        accessKeyId,
   *        accessKeySecret,
   *        StorageLevel.MEMORY_AND_DISK_2,
   *        ReadMessage);
   *
   *     onsStream.foreachRDD(rdd => {
   *       ...
   *     });
   * }}}
   * @param jssc Java streamingContext object.
   * @param consumerId Name of a set of consumers.
   * @param topic Which topic to subscribe.
   * @param tags Which tag to subscribe.
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param func Extract information from ONS message.
   * @return
   */
  @Experimental
  def createStream(
      jssc: JavaStreamingContext,
      consumerId: String,
      topic: String,
      tags: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel,
      func: JFunction[Message, Array[Byte]]): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, consumerId, topic, tags, accessKeyId, accessKeySecret, storageLevel,
      (msg: Message) => func.call(msg))
  }

  private def extractMessage(msg: Message): Array[Byte] = msg.toString.getBytes

  @Experimental
  def createDefaultStreams(
      jssc: JavaStreamingContext,
      consumerId: String,
      topic: String,
      tags: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    createStream(jssc.ssc, consumerId, topic, tags, accessKeyId, accessKeySecret, storageLevel, extractMessage _)
  }
}

class OnsUtilsHelper {

  def createDefaultStreams(
      jssc: JavaStreamingContext,
      consumerId: String,
      topic: String,
      tags: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): JavaReceiverInputDStream[Array[Byte]] = {
    OnsUtils.createDefaultStreams(jssc, consumerId, topic, tags, accessKeyId, accessKeySecret, storageLevel)
  }
}