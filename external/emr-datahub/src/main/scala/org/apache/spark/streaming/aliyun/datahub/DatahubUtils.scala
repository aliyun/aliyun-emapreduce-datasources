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
package org.apache.spark.streaming.aliyun.datahub

import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.{DatahubClient, DatahubConfiguration}
import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object DatahubUtils {
  def createStream(
      ssc: StreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      shardId: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    ssc.withNamedScope("datahub stream") {
      new DatahubDStream(
        ssc,
        projectName,
        topicName,
        subId,
        accessKeyId,
        accessKeySecret,
        endpoint,
        shardId,
        func,
        storageLevel)
    }
  }

  def createStream(
      ssc: StreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    val account = new AliyunAccount(accessKeyId, accessKeySecret)
    val conf = new DatahubConfiguration(account, endpoint)
    val loghubClient = new DatahubClient(conf)
    import scala.collection.JavaConverters._
    // no need to catch exception or retry if datahub-service error
    val shardEntries = loghubClient.listShard(projectName, topicName).getShards.asScala
    var dStream: DStream[Array[Byte]] = null

    for (shardEntry <- shardEntries) {
      if (dStream == null) {
        dStream = createStream(ssc, projectName, topicName, subId, accessKeyId, accessKeySecret, endpoint,
          shardEntry.getShardId, func, storageLevel)
      } else {
        dStream = dStream.union(createStream(ssc, projectName, topicName, subId, accessKeyId, accessKeySecret, endpoint,
          shardEntry.getShardId, func, storageLevel))
      }
    }

    dStream
  }
}
