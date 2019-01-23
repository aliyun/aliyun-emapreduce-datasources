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

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConversions._

import com.aliyun.datahub.model.{OffsetContext, RecordEntry}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator

private class DatahubIterator(
    datahubClient: DatahubClientAgent,
    endpoint: String,
    project: String,
    topic: String,
    shardId: String,
    cursor: String,
    count: Int,
    subId: String,
    accessId: String,
    accessKey: String,
    func: RecordEntry => String,
    zkClient: ZkClient,
    checkpointDir: String,
    context: TaskContext) extends NextIterator[Array[Byte]] with Logging{
  private val datahubShardMaxSize = 512
  private val step = 100
  private var dataBuffer = new LinkedBlockingQueue[Array[Byte]](datahubShardMaxSize * step * 10)
  private var hasRead = 0
  private var lastOffset: OffsetContext.Offset = null
  private val inputMetrcis = context.taskMetrics().inputMetrics
  private var nextCursor = cursor

  override protected def getNext(): Array[Byte] = {
    finished = !checkHasNext
    if (!finished) {
      if (dataBuffer.isEmpty) {
        fetchData()
      }
      dataBuffer.poll()
    } else {
      Array.empty[Byte]
    }
  }

  override protected def close() = {
    try {
      inputMetrcis.incRecordsRead(hasRead)
      dataBuffer.clear()
      dataBuffer = null
    } catch {
      case e: Exception =>
        logError("Catch exception when close datahub iterator.", e)
    }
  }

  private def checkHasNext: Boolean = {
    val hasNext = hasRead < count || !dataBuffer.isEmpty
    if (!hasNext) {
      // commit next offset
      lastOffset.setSequence(lastOffset.getSequence + 1)
      writeDataToZk(zkClient, s"$checkpointDir/datahub/commit/$project/$topic/$subId/$shardId",
        JacksonParser.toJsonNode(lastOffset).toString)
    }
    hasNext
  }

  private def fetchData() = {
    val topicResult = datahubClient.getTopic(project, topic)
    val schema = topicResult.getRecordSchema
    val limit = if (count - hasRead >= step) step else count - hasRead
    val recordResult = datahubClient.getRecords(project, topic, shardId, nextCursor, limit, schema)
    val num = recordResult.getRecordCount
    if (num == 0) {
      logDebug("Fetch 0 records from datahub, sleep 100ms and fetch again")
      Thread.sleep(100)
    } else {
      recordResult.getRecords.foreach(record => {
        dataBuffer.offer(func(record).getBytes())
        lastOffset = record.getOffset
      })
      nextCursor = recordResult.getNextCursor
      hasRead = hasRead + num
      logDebug(s"shardId: $shardId, nextCursor: $nextCursor, hasRead: $hasRead, count: $count")
    }
  }

  private def writeDataToZk(zkClient: ZkClient, path:String, data:String) = {
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true)
    }
    zkClient.writeData(path, data)
  }
}
