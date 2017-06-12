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

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream

/**
 * Various utility classes for working with Aliyun DTS.
 */
object DtsUtils extends Logging {

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

  def defaultMessageFunc(message: ClusterMessage): String = {
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
