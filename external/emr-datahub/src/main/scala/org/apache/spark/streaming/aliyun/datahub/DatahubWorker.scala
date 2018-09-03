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

import com.aliyun.datahub.exception.{OffsetResetedException, OffsetSessionChangedException, SubscriptionOfflineException}

import scala.collection.JavaConversions._
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.model.RecordEntry
import com.aliyun.datahub.DatahubConfiguration
import org.apache.spark.internal.Logging

class DatahubWorker(projectName: String, topicName: String, shardId: String, subId: String,
                    conf: DatahubConfiguration, receiver: DatahubReceiver, func: RecordEntry => String)
    extends Runnable with Logging {
  val client = new DatahubClientAgent(conf)
  override def run(): Unit = {
    var recordNum = 0L
    val topicResult = client.getTopic(projectName, topicName)
    val offsetCtx = client.initOffsetContext(projectName, topicName, subId, shardId)
    var cursor: String = null
    if (!offsetCtx.hasOffset) {
      val cursorResult = client.getCursor(projectName, topicName, shardId, CursorType.OLDEST)
      cursor = cursorResult.getCursor
    } else {
      cursor = client.getNextOffsetCursor(offsetCtx).getCursor
    }
    logInfo(s"Start consume records, begin offset context ${offsetCtx.toObjectNode.toString}, cursor: $cursor")
    while (true) {
      try {
        val recordResult = client.getRecords(projectName, topicName, shardId, cursor, 1000,
          topicResult.getRecordSchema)
        val records = recordResult.getRecords
        if (records.size() == 0) {
          Thread.sleep(100)
        } else {
          for (record <- records) {
            offsetCtx.setOffset(record.getOffset)
            recordNum += 1
            receiver.store(func(record).getBytes)
            if (recordNum % 100 == 0) {
              client.commitOffset(offsetCtx)
            }
          }
          cursor = recordResult.getNextCursor
        }
      } catch {
        case _: OffsetResetedException =>
          client.updateOffsetContext(offsetCtx)
          cursor = client.getNextOffsetCursor(offsetCtx).getCursor
          logInfo(s"Restart consume shard: $shardId, reset ${offsetCtx.toObjectNode.toString}, cursor: $cursor")
        case e: Exception =>
         throw e
      }
    }
  }
}
