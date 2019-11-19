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
    logGroupStep: Int = 100)
  extends NextIterator[String] with Logging {

  private var hasRead: Int = 0
  private var nextCursor: String = startCursor
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

  def fetchNextBatch(): Unit = {
    // scalastyle:off
    import scala.collection.JavaConversions._
    // scalastyle:on
    val batchGetLogRes: BatchGetLogResponse =
      mClient.BatchGetLog(project, logStore, shardId, logGroupStep, nextCursor, endCursor)
    var count = 0
    batchGetLogRes.GetLogGroups().foreach(group => {
      group.GetLogGroup().getLogsList.foreach(log => {
        val topic = group.GetTopic()
        val source = group.GetSource()
        val obj = new JSONObject()
        obj.put(__TIME__, Integer.valueOf(log.getTime))
        obj.put(__TOPIC__, topic)
        obj.put(__SOURCE__, source)
        log.getContentsList.foreach(content => {
          obj.put(content.getKey, content.getValue)
        })

        val flg = group.GetFastLogGroup()
        for (i <- 0 until flg.getLogTagsCount) {
          obj.put("__tag__:".concat(flg.getLogTags(i).getKey), flg.getLogTags(i).getValue)
        }

        count += 1
        logData.offer(obj.toJSONString)
      })
    })
    val crt = nextCursor
    nextCursor = batchGetLogRes.GetNextCursor()
    logDebug(s"shardId: $shardId, currentCursor: $crt, nextCursor: $nextCursor," +
      s" endCursor: $endCursor, hasRead: $hasRead, count: $count," +
      s" get: $count, queue: ${logData.size()}")
  }
}