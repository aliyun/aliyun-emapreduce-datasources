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

import java.util.Base64
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import com.aliyun.openservices.log.response.{BatchGetLogResponse, GetCursorResponse}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.aliyun.logservice.LoghubClientAgent
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.util.NextIterator

class LoghubSourceRDD(
      @transient sc: SparkContext,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      shardOffsets: ArrayBuffer[(Int, Int, Int)],
      schemaFieldNames: Array[String],
      sourceOptions: Map[String, String])
    extends RDD[LoghubData](sc, Nil) with Logging {

  @transient var mClient: LoghubClientAgent =
    LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)

  val step = sourceOptions.getOrElse("loghub.batchGet.step", "100").toInt

  private def initialize(): Unit = {
    mClient = LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[LoghubData] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    val schemaFieldPos: Map[String, Int] = schemaFieldNames.zipWithIndex.toMap
    try {
      new InterruptibleIterator[LoghubData](context, new NextIterator[LoghubData]() {
        val startCursor: GetCursorResponse =
          mClient.GetCursor(project, logStore, shardPartition.shardId, shardPartition.startCursor)
        val endCursor: GetCursorResponse =
          mClient.GetCursor(project, logStore, shardPartition.shardId, shardPartition.endCursor)
        private var hasRead: Int = 0
        private val nextCursor: GetCursorResponse = startCursor
        private var nextCursorTime = nextCursor.GetCursor()
        private var nextCursorNano: Long = new String(Base64.getDecoder.decode(nextCursorTime)).toLong
        private val endCursorNano: Long = new String(Base64.getDecoder.decode(endCursor.GetCursor())).toLong
        // TODO: This may cost too much memory.
        private var logData = new LinkedBlockingQueue[LoghubData](4096 * step)
        private val inputMetrics = context.taskMetrics.inputMetrics

        context.addTaskCompletionListener {
          _ => closeIfNeeded()
        }

        fetchNextBatch()

        def checkHasNext(): Boolean = nextCursorNano < endCursorNano || logData.nonEmpty

        override protected def getNext(): LoghubData = {
          finished = !checkHasNext()
          if (!finished) {
            if (logData.isEmpty) {
              fetchNextBatch()
            }
            if (logData.isEmpty) {
              finished = true
              null.asInstanceOf[LoghubData]
            } else {
              hasRead += 1
              logData.poll()
            }
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
            shardPartition.shardId, step, nextCursorTime, endCursor.GetCursor())
          var count = 0
          batchGetLogRes.GetLogGroups().foreach(group => {
            group.GetLogGroup().getLogsList.foreach(log => {
              val topic = group.GetTopic()
              val source = group.GetSource()
              try {
                val columnArray = Array.tabulate(schemaFieldNames.length)(_ =>
                  (null, null).asInstanceOf[(String, String)]
                )
                log.getContentsList
                  .filter(content => schemaFieldPos.contains(content.getKey))
                  .foreach(content => {
                    columnArray(schemaFieldPos(content.getKey)) = (content.getKey, content.getValue)
                  })

                val flg = group.GetFastLogGroup()
                for (i <- 0 until flg.getLogTagsCount) {
                  val tagKey = flg.getLogTags(i).getKey
                  val tagValue = flg.getLogTags(i).getValue
                  if (schemaFieldPos.contains(tagKey)) {
                    columnArray(schemaFieldPos(tagKey)) = (tagKey, tagValue)
                  }
                }

                if (schemaFieldPos.contains(__PROJECT__)) {
                  columnArray(schemaFieldPos(__PROJECT__)) = (__PROJECT__, project)
                }

                if (schemaFieldPos.contains(__STORE__)) {
                  columnArray(schemaFieldPos(__STORE__)) = (__STORE__, logStore)
                }

                if (schemaFieldPos.contains(__SHARD__)) {
                  columnArray(schemaFieldPos(__SHARD__)) = (__SHARD__, shardPartition.shardId.toString)
                }

                if (schemaFieldPos.contains(__TOPIC__)) {
                  columnArray(schemaFieldPos(__TOPIC__)) = (__TOPIC__, topic)
                }

                if (schemaFieldPos.contains(__SOURCE__)) {
                  columnArray(schemaFieldPos(__SOURCE__)) = (__SOURCE__, source)
                }

                if (schemaFieldPos.contains(__TIME__)) {
                  columnArray(schemaFieldPos(__TIME__)) = (__TIME__, new java.sql.Timestamp(log.getTime * 1000L).toString)
                }

                count += 1
                logData.offer(new SchemaLoghubData(columnArray))
              } catch {
                case e: NoSuchElementException =>
                  logWarning(s"Meet an unknown column name, ${e.getMessage}. Treat this as an invalid " +
                    s"data and continue.")
              }
            })
          })

          val crt = nextCursorTime
          nextCursorTime = batchGetLogRes.GetNextCursor()
          nextCursorNano = new String(Base64.getDecoder.decode(nextCursorTime)).toLong
          logDebug(s"project: $project, logStore: $logStore, shardId: ${shardPartition.shardId}, " +
            s"startCursor: ${shardPartition.startCursor}, endCursor: ${shardPartition.endCursor}, " +
            s"currentCursorTime: $crt, nextCursorTime: $nextCursorTime, " +
            s"nextCursorNano: $nextCursorNano, endCursorNano: $endCursorNano, " +
            s"hasRead: $hasRead, batchGet: $count, queueSize: ${logData.size()}")
        }
      })
    } catch {
      case _: Exception =>
        Iterator.empty.asInstanceOf[Iterator[LoghubData]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val logGroupStep = sc.getConf.get("spark.loghub.batchGet.step", "100").toInt
    shardOffsets.zipWithIndex.map { case (p, idx) =>
      new ShardPartition(id, idx, p._1, project, logStore,
        accessKeyId, accessKeySecret, endpoint, p._2, p._3, logGroupStep).asInstanceOf[Partition]
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
      val startCursor: Int,
      val endCursor: Int,
      val logGroupStep: Int = 100) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId
    override def index: Int = partitionId
  }
}
