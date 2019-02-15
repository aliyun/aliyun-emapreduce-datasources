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
package org.apache.spark.sql.aliyun.logservice

import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import com.aliyun.openservices.log.response.BatchGetLogResponse
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.streaming.aliyun.logservice.{DirectLoghubInputDStream, LoghubClientAgent}
import org.apache.spark.util.NextIterator

class LoghubSourceRDD(
      @transient sc: SparkContext,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      shardOffsets: ArrayBuffer[(Int, String, String)],
      schemaFieldNames: Array[String],
      zkParams: Map[String, String] = null,
      checkpointDir: String = null,
      maxOffsetsPerTrigger: Long = -1L,
      fallback: Boolean = false)
    extends RDD[LoghubData](sc, Nil) with Logging {

  @transient var mClient: LoghubClientAgent =
    LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
  (zkParams, checkpointDir, maxOffsetsPerTrigger) match {
    case (null, null, d) if d < 0L => logDebug("Created LoghubSourceRDD without zk checkpoint")
    case (zk, cp, d) if zk != null && cp != null && d > 0L => logDebug("Created LoghubSourceRDD with zk checkpoint")
    case _ => throw new IllegalArgumentException("Illegal argument (zkParams, checkpointDir, duration), we " +
      s"should set them or not, but current setting is (${zkParams==null}, ${checkpointDir==null}, ${maxOffsetsPerTrigger<0L})")
  }

  private def initialize(): Unit = {
    mClient = LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[LoghubData] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    val schemaFieldPos: Map[String, Int] = schemaFieldNames
      .filter(fieldName => !isDefaultField(fieldName))
      .map(fieldName => {
        if (fieldName.startsWith("__tag")) {
          fieldName.substring(5)
        } else {
          fieldName
        }
      }).zipWithIndex.toMap
    try {
      new InterruptibleIterator[LoghubData](context, new NextIterator[LoghubData]() {
        val zkClient: ZkClient = LoghubOffsetReader.getOrCreateZKClient(zkParams)

        private val count = shardPartition.count
        private val step: Int = 1000
        private var hasRead: Int = 0
        private var nextCursor: String = shardPartition.startCursor
        // TODO: This may cost too much memory.
        private var logData = new LinkedBlockingQueue[LoghubData](4096 * step)

        private val inputMetrics = context.taskMetrics.inputMetrics

        context.addTaskCompletionListener {
          _ => closeIfNeeded()
        }

        def checkHasNext(): Boolean = {
          if (count < 0) {
            !nextCursor.equals(shardPartition.endCursor) || logData.nonEmpty
          } else {
            val hasNext = (hasRead < count && !nextCursor.equals(shardPartition.endCursor)) || logData.nonEmpty
            if (!hasNext) {
              DirectLoghubInputDStream.writeDataToZK(zkClient,
                s"$checkpointDir/available/$project/$logStore/${shardPartition.shardId}.shard", nextCursor)
            }
            hasNext
          }
        }

        override protected def getNext(): LoghubData = {
          finished = !checkHasNext()
          if (!finished) {
            if (logData.isEmpty) {
              fetchNextBatch()
            }

            hasRead += 1
            logData.poll()
          } else {
            null.asInstanceOf[LoghubData]
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
          val batchGetLogRes: BatchGetLogResponse = mClient.BatchGetLog(project, logStore,
            shardPartition.shardId, step, nextCursor, shardPartition.endCursor)
          var count = 0
          batchGetLogRes.GetLogGroups().foreach(group => {
            group.GetLogGroup().getLogsList.foreach(log => {
              val topic = group.GetTopic()
              val source = group.GetSource()
              if (!fallback) {
                try {
                  // the first six columns: logProject, logStore, shardId, dataTime, topic, source
                  // the length of rest of columns: numCols - 6
                  val columnArray = Array.tabulate(schemaFieldNames.length - 6)(idx =>
                    (null, null).asInstanceOf[(String, Any)]
                  )
                  log.getContentsList.foreach(content => {
                    columnArray(schemaFieldPos(content.getKey)) = (content.getKey, content.getValue)
                  })

                  val flg = group.GetFastLogGroup()
                  for (i <- 0 until flg.getLogTagsCount) {
                    val tagKey = flg.getLogTags(i).getKey
                    val tagValue = flg.getLogTags(i).getValue
                    if (!tagKey.equals(__PACK_ID__) && !tagKey.equals(__USER_DEFINED_ID__)) {
                      columnArray(schemaFieldPos(tagKey)) = (tagKey, tagValue)
                    }
                  }

                  count += 1
                  logData.offer(new SchemaLoghubData(project, logStore, shardPartition.shardId,
                    new java.sql.Timestamp(log.getTime * 1000L), topic,
                    source, columnArray))
                } catch {
                  case e: NoSuchElementException =>
                    logWarning(s"Meet an unknown column name, ${e.getMessage}. Treat this as an invalid " +
                      s"data and continue.")
                }
              } else {
                val obj = new JSONObject()
                log.getContentsList.foreach(content => {
                  obj.put(content.getKey, content.getValue)
                })

                val flg = group.GetFastLogGroup()
                for (i <- 0 until flg.getLogTagsCount) {
                  obj.put(flg.getLogTags(i).getKey, flg.getLogTags(i).getValue)
                }
                count += 1
                logData.offer(new RawLoghubData(project, logStore, shardPartition.shardId,
                  new java.sql.Timestamp(log.getTime * 1000L), topic,
                  source, obj.toJSONString.getBytes))
              }
            })
          })

          val crt = nextCursor
          nextCursor = batchGetLogRes.GetNextCursor()
          logDebug(s"shardId: ${shardPartition.shardId}, currentCursor: $crt, nextCursor: $nextCursor," +
            s" endCursor: ${shardPartition.endCursor}, hasRead: $hasRead, count: $count," +
            s" get: $count, queue: ${logData.size()}")
        }
      })
    } catch {
      case _: Exception =>
        Iterator.empty.asInstanceOf[Iterator[LoghubData]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    shardOffsets.zipWithIndex.map { case (p, idx) =>
      new ShardPartition(id, idx, p._1, project, logStore,
        accessKeyId, accessKeySecret, endpoint, p._2, p._3, maxOffsetsPerTrigger).asInstanceOf[Partition]
    }.toArray
  }

  private class ShardPartition(
      rddId: Int,
      partitionId: Int,
      val shardId: Int,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      val startCursor: String,
      val endCursor: String,
      val count: Long = -1L) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId
    override def index: Int = partitionId
  }
}
