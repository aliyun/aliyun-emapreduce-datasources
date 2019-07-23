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

import com.aliyun.openservices.log.common.LogGroupData
import com.aliyun.openservices.log.response.BatchGetLogResponse
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

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
    logGroupStep: Int = 100,
    logGroupDecoder: LogGroupData => ArrayBuffer[String])
  extends NextIterator[String] with Logging {

  private var hasRead: Int = 0
  private var nextCursor: String = startCursor
  // TODO: This may cost too much memory.
  private var logData = new LinkedBlockingQueue[String](4096 * logGroupStep)
  private val decoder = logGroupDecoder

  val inputMetrics = context.taskMetrics.inputMetrics

  context.addTaskCompletionListener {
    context => closeIfNeeded()
  }

  def checkHasNext(): Boolean = {
    val hasNext = (hasRead < count) && !nextCursor.equals(endCursor) || logData.nonEmpty
    if (!hasNext) {
      DirectLoghubInputDStream.writeDataToZK(zkClient, s"$checkpointDir/commit/$project/$logStore/$shardId.shard", nextCursor)
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
    val batchGetLogRes: BatchGetLogResponse = mClient.BatchGetLog(project, logStore, shardId, logGroupStep, nextCursor, endCursor)
    var count = 0
    batchGetLogRes.GetLogGroups().foreach(group => {
      val r = decoder(group)
      logData.addAll(r)
      count += r.size()
    })
    val crt = nextCursor
    nextCursor = batchGetLogRes.GetNextCursor()
    logDebug(s"shardId: $shardId, currentCursor: $crt, nextCursor: $nextCursor," +
      s" endCursor: $endCursor, hasRead: $hasRead, count: $count," +
      s" get: $count, queue: ${logData.size()}")
  }
}