/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.spark.streaming.aliyun.dts

import scala.collection.JavaConversions._

import com.alibaba.fastjson.JSONObject
import com.aliyun.drc.clusterclient.message.ClusterMessage

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * Various utility classes for working with Aliyun DTS.
 */
object DtsUtils extends Logging {

  /**
   * Create an input stream that pulls message from a Aliyun DTS stream.
   * {{{
   *    val ssc: StreamingSparkContext = ...
   *    val accessKeyId = "kj7aY*******UYx6"
   *    val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *    val guid = "dts-guid-name"
   *
   *    def func: ClusterMessage => String = msg => msg.getRecord.toString
   *
   *    val dtsStream = DtsUtils.createStream(
   *      ssc,
   *      accessKeyId,
   *      accessKeySecret,
   *      guid,
   *      func,
   *      StorageLevel.MEMORY_AND_DISK_2,
   *      false)
   *
   *    dtsStream.foreachRDD(rdd => {
   *      ...
   *    })
   *
   * }}}
   * @param ssc StreamingContext object.
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param guid Aliyun DTS guid name.
   * @param func Extract information from DTS record message.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param usePublicIp use public ip or not.
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      accessKeyId: String,
      accessKeySecret: String,
      guid: String,
      func: ClusterMessage => String,
      storageLevel: StorageLevel,
      usePublicIp: Boolean): ReceiverInputDStream[String] = {
    new BinlogDStream(ssc, accessKeyId, accessKeySecret, guid, func, storageLevel, usePublicIp)
  }

  /**
   * Create an input stream that pulls message from a Aliyun DTS stream.
   * {{{
   *    val ssc: StreamingSparkContext = ...
   *    val accessKeyId = "kj7aY*******UYx6"
   *    val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *    val guid = "dts-guid-name"
   *
   *    val dtsStream = DtsUtils.createStream(
   *      ssc,
   *      accessKeyId,
   *      accessKeySecret,
   *      guid,
   *      StorageLevel.MEMORY_AND_DISK_2,
   *      false)
   *
   *    dtsStream.foreachRDD(rdd => {
   *      ...
   *    })
   *
   * }}}
   * @param ssc StreamingContext object.
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param guid Aliyun DTS guid name.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param usePublicIp use public ip or not.
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      accessKeyId: String,
      accessKeySecret: String,
      guid: String,
      storageLevel: StorageLevel,
      usePublicIp: Boolean): ReceiverInputDStream[String] = {
    new BinlogDStream(ssc, accessKeyId, accessKeySecret, guid, defaultMessageFunc, storageLevel, usePublicIp)
  }

  /**
   * Create an input stream that pulls message from a Aliyun DTS stream.
   * {{{
   *    JavaStreamingContext jssc = ...;
   *    String accessKeyId = "kj7aY*******UYx6";
   *    String accessKeySecret = "AiNMAlxz*************1PxaPaL8t";
   *    String guid = "dts-guid-name";
   *
   *    static class ReadMessage implements Function<ClusterMessage, String> {
   *        @Override
   *        public String call(ClusterMessage msg) {
   *            return msg.getRecord.toString;
   *        }
   *    }
   *
   *    JavaReceiverInputDStream<String> dtsStream = DtsUtils.createStream(
   *        ssc,
   *        accessKeyId,
   *        accessKeySecret,
   *        guid,
   *        ReadMessage,
   *        StorageLevel.MEMORY_AND_DISK_2,
   *        false);
   *
   *    dtsStream.foreachRDD(rdd => {
   *        ...
   *    });
   * }}}
   * @param jssc Java streamingContext object.
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param guid Aliyun DTS guid name.
   * @param func Extract information from DTS record message.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param usePublicIp use public ip or not.
   * @return
   */
  @Experimental
  def createStream(
      jssc: JavaStreamingContext,
      accessKeyId: String,
      accessKeySecret: String,
      guid: String,
      func: JFunction[ClusterMessage, String],
      storageLevel: StorageLevel,
      usePublicIp: Boolean): JavaReceiverInputDStream[String] = {
    createStream(jssc.ssc, accessKeyId, accessKeySecret, guid, (msg: ClusterMessage) => func.call(msg),
      storageLevel, usePublicIp)
  }

  /**
   * Create an input stream that pulls message from a Aliyun DTS stream.
   * {{{
   *    JavaStreamingContext jssc = ...;
   *    String accessKeyId = "kj7aY*******UYx6";
   *    String accessKeySecret = "AiNMAlxz*************1PxaPaL8t";
   *    String guid = "dts-guid-name";
   *
   *    JavaReceiverInputDStream<String> dtsStream = DtsUtils.createStream(
   *        ssc,
   *        accessKeyId,
   *        accessKeySecret,
   *        guid,
   *        StorageLevel.MEMORY_AND_DISK_2,
   *        false);
   *
   *    dtsStream.foreachRDD(rdd => {
   *        ...
   *    });
   * }}}
   * @param jssc Java streamingContext object.
   * @param accessKeyId Aliyun Access Key ID.
   * @param accessKeySecret Aliyun Access Key Secret.
   * @param guid Aliyun DTS guid name.
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @param usePublicIp use public ip or not.
   * @return
   */
  @Experimental
  def createStream(
      jssc: JavaStreamingContext,
      accessKeyId: String,
      accessKeySecret: String,
      guid: String,
      storageLevel: StorageLevel,
      usePublicIp: Boolean): JavaReceiverInputDStream[String] = {
    createStream(jssc.ssc, accessKeyId, accessKeySecret, guid, storageLevel, usePublicIp)
  }

  private def defaultMessageFunc(message: ClusterMessage): String = {
    try {
      val obj = new JSONObject()
      message.getRecord.getAttributes.foreach(attribute => {
        obj.put(attribute._1, attribute._2)
      })
      message.getRecord.getFieldList.foreach(field => {
        val fieldObj = new JSONObject()
        fieldObj.put("Field name", field.name)
        fieldObj.put("Field type", field.`type`)
        fieldObj.put("Field length", field.length)
        if (field.name != null) {
          if (field.encoding.equals("binary")) {
            fieldObj.put("Field value(binary)", field.getValue.getBytes)
          } else {
            fieldObj.put("Field value", field.getValue.toString(field.encoding))
          }
        } else {
          fieldObj.put("Field value", "null")
        }

        obj.put("field", fieldObj.toJSONString)
      })
      obj.toJSONString
    } catch {
      case e: Exception =>
        logError("Failed to resolve dts message.")
        throw e
    }
  }
}