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

import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.exception.LogException
import com.aliyun.openservices.log.response.BatchGetLogResponse
import org.I0Itec.zkclient.ZkClient

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.util.NextIterator

class LoghubIterator(
    zkClient: ZkClient,
    mClient: LoghubClientAgent,
    project: String,
    logStore: String,
    shardId: Int,
    startCursor: String,
    endCursor: String,
    count: Int,
    checkpointDir: String,
    context: TaskContext,
    shardReadOnly: Boolean = false,
    logGroupStep: Int = 100)
  extends NextIterator[String] with Logging {

  private var hasRead: Int = 0
  private var nextCursor: String = startCursor
  private var shardDeleted: Boolean = false
  // TODO: This may cost too much memory.
  private var logData = new LinkedBlockingQueue[String](4096 * logGroupStep)

  val inputMetrics = context.taskMetrics.inputMetrics

  context.addTaskCompletionListener {
    context => closeIfNeeded()
  }

  def checkHasNext(): Boolean = {
    // scalastyle:off
    import scala.collection.JavaConversions._
    // scalastyle:on
    if (shardDeleted) {
      return false
    }
    val hasNext = (hasRead < count) && !nextCursor.equals(endCursor) || logData.nonEmpty
    if (!hasNext) {
      DirectLoghubInputDStream.writeDataToZK(zkClient,
        s"$checkpointDir/commit/$project/$logStore/$shardId.shard", nextCursor)
    }

    hasNext
  }

  override protected def getNext(): String = {
    finished = !checkHasNext()
    if (!finished) {
      if (logData.isEmpty) {
        fetchNextBatch()
      }
      if (logData.isEmpty) {
        finished = true
        null
      } else {
        hasRead += 1
        logData.poll()
      }
    } else {
      null
    }
  }

  override def close() {
    try {
      inputMetrics.incBytesRead(hasRead)
      logData.clear()
      logData = null
    } catch {
      case e: Exception => logWarning("Exception when close LoghubIterator.", e)
    }
  }

  def fetchBatch(): BatchGetLogResponse = {
    try {
      return mClient.BatchGetLog(project, logStore, shardId,
        logGroupStep, nextCursor, endCursor)
    } catch {
      case e: LogException =>
        if (shardReadOnly && e.GetErrorCode().equals("ShardNotExist")) {
          shardDeleted = true
          logWarning(s"The read only shard $shardId has been deleted")
        } else {
          throw e
        }
    }
    null
  }

  def fetchNextBatch(): Unit = {
    // scalastyle:on
    val response: BatchGetLogResponse = fetchBatch()
    if (response != null) {
      parseResponseAndMoveToNextCursor(response)
    }
  }

  def parseResponseAndMoveToNextCursor(response: BatchGetLogResponse): Unit = {
    // scalastyle:off
    import scala.collection.JavaConversions._
    var count = 0
    response.GetLogGroups().foreach(group => {
      val fastLogGroup = group.GetFastLogGroup()
      val logCount = fastLogGroup.getLogsCount
      val topic = fastLogGroup.getTopic
      val source = fastLogGroup.getSource
      for (i <- 0 until logCount) {
        val record = fastLogGroup.getLogs(i)
        val obj = new JSONObject()
        obj.put(__TIME__, record.getTime)
        obj.put(__TOPIC__, topic)
        obj.put(__SOURCE__, source)
        val fieldCount = record.getContentsCount
        for (j <- 0 until fieldCount) {
          val f = record.getContents(j)
          obj.put(f.getKey, f.getValue)
        }
        val tagCount = fastLogGroup.getLogTagsCount
        for (j <- 0 until tagCount) {
          val tag = fastLogGroup.getLogTags(j)
          obj.put("__tag__:".concat(tag.getKey), tag.getValue)
        }
        count += 1
        logData.offer(obj.toJSONString)
      }
    })
    val currentCursor = nextCursor
    nextCursor = response.GetNextCursor()
    logDebug(s"shardId: $shardId, currentCursor: $currentCursor, nextCursor: $nextCursor," +
      s" endCursor: $endCursor, hasRead: $hasRead, count: $count," +
      s" get: $count, queue: ${logData.size()}")
  }
}