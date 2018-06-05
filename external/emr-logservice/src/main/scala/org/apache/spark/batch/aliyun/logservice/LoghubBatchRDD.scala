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
package org.apache.spark.batch.aliyun.logservice

import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.Client
import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.exception.LogException
import com.aliyun.openservices.log.response.BatchGetLogResponse
import org.apache.http.conn.ConnectTimeoutException
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

class LoghubBatchRDD(
    @transient sc:SparkContext,
    project: String,
    logStore: String,
    accessId: String,
    accessKey: String,
    endpoint: String,
    startTime: Long,
    endTime: Long) extends RDD[String](sc, Nil) {

  def this(@transient sc: SparkContext, project: String, logStore: String, accessId: String, accessKey: String, endpoint: String, startTime: Long) = {
    this(sc, project, logStore, accessId, accessKey, endpoint, startTime, -1L)
  }

  @transient val client: Client = LoghubBatchRDD.getClient(endpoint, accessId, accessKey)

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    try {
      val partittion = split.asInstanceOf[ShardPartition]
      new InterruptibleIterator[String](context, new LoghubIterator(LoghubBatchRDD.getClient(endpoint, accessId, accessKey), partittion.index, project, logStore, partittion.startCursor, partittion.endCursor, context))
    } catch {
      case e: Exception => Iterable.empty.asInstanceOf[Iterator[String]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    import scala.collection.JavaConversions._
    val shards = client.ListShard(project, logStore).GetShards()
    shards.map(shard => {
      val shardId = shard.GetShardId()
      val startCursor = client.GetCursor(project, logStore, shardId, startTime).GetCursor()
      val endCursor = endTime match {
        case -1 => {
          client.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
        }
        case end => client.GetCursor(project, logStore, shardId, end).GetCursor()
      }
      new ShardPartition(id, shardId, project, logStore, accessId, accessKey, endpoint, startCursor, endCursor).asInstanceOf[Partition]
    }).toArray.sortWith(_.index < _.index)
  }

  private class ShardPartition(
      rddId: Int,
      shardId: Int,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKey: String,
      endpoint: String,
      val startCursor: String,
      val endCursor: String) extends Partition {

    override def hashCode(): Int = 41 * (41 + rddId) + shardId

    override def index: Int = shardId
  }

  private class LoghubIterator(
      client: Client,
      shardId: Int,
      project: String,
      logStore: String,
      startCursor: String,
      endCursor: String,
      context: TaskContext) extends NextIterator[String] {
    val logGroupCount = 1000
    var logCache = new LinkedBlockingQueue[String]()
    var curCursor: String = startCursor

    context.addTaskCompletionListener(context => closeIfNeeded())

    override protected def getNext(): String = {
      if (logCache.isEmpty) {
        if (curCursor.equals(endCursor)) {
          finished = true
          return ""
        }
        fetchData()
      }

      logCache.poll()
    }

    override protected def close(): Unit = {
      logCache.clear()
      logCache = null
    }

    def fetchData(): Unit = {
      var logData : BatchGetLogResponse = null
      val logServiceTimeoutMaxRetry = 3
      var failed = true
      var retry = 0
      var currentException :Exception = null

      while (retry <= logServiceTimeoutMaxRetry && failed) {
        try {
          logData = client.BatchGetLog(project, logStore, shardId, logGroupCount, curCursor, endCursor)
          failed = false
        } catch {
          case e: LogException => {
            if (e.getCause != null && e.getCause.getCause != null && e.getCause.getCause.isInstanceOf[ConnectTimeoutException]) {
              retry += 1
              currentException = e
            } else {
              throw e
            }
          }
          case e => throw e
        }
      }
      if (retry > logServiceTimeoutMaxRetry) {
        logError("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].")
        throw currentException
      }

      import scala.collection.JavaConversions._
      logData.GetLogGroups().foreach(logGroup => {
        logGroup.GetLogGroup().getLogsList.foreach(log => {
          val data = new JSONObject()
          data.put("__time__", log.getTime)
          data.put("__topic__", logGroup.GetTopic())
          data.put("__source__", logGroup.GetSource())
          log.getContentsList.foreach( content => {
            data.put(content.getKey, content.getValue)
          })
          val flg = logGroup.GetFastLogGroup()
          for (i <- 0 until flg.getLogTagsCount) {
            data.put("__tag__:".concat(flg.getLogTags(i).getKey), flg.getLogTags(i).getValue)
          }

          logCache.offer(data.toString)
        })
      })
      curCursor = logData.GetNextCursor()
    }
  }
}

object LoghubBatchRDD {
  private var client: Client = _

  def getClient(endpoint: String, accessId: String, accessKey: String): Client = {
    if (client == null) {
      client = new Client(endpoint, accessId, accessKey)
    }

    client
  }
}
