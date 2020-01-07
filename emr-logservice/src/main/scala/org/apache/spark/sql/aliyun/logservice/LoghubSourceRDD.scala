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

// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on
import scala.collection.mutable.ArrayBuffer

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.response.{BatchGetLogResponse, GetCursorResponse}
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.aliyun.logservice.LoghubClientAgent
import org.apache.spark.util.NextIterator

class LoghubSourceRDD(
      @transient sc: SparkContext,
      shardOffsets: ArrayBuffer[(Int, Int, Int)],
      schemaFieldNames: Array[String],
      schemaDDL: String,
      defaultSchema: Boolean,
      sourceOptions: Map[String, String])
    extends RDD[InternalRow](sc, Nil) with Logging {
  private val logProject = sourceOptions.getOrElse("sls.project",
    throw new MissingArgumentException("Missing logService project (='sls.project')."))
  private val logStore = sourceOptions.getOrElse("sls.store",
    throw new MissingArgumentException("Missing logService store (='sls.store')."))
  private val accessKeyId = sourceOptions.getOrElse("access.key.id",
    throw new MissingArgumentException("Missing access key id (='access.key.id')."))
  private val accessKeySecret = sourceOptions.getOrElse("access.key.secret",
    throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
  private val endpoint = sourceOptions.getOrElse("endpoint",
    throw new MissingArgumentException("Missing log store endpoint (='endpoint')."))

  @transient var mClient: LoghubClientAgent =
    LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)

  val step = sourceOptions.getOrElse("loghub.batchGet.step", "100").toInt
  private val appendSequenceNumber: Boolean =
    sourceOptions.getOrElse("appendSequenceNumber", "false").toBoolean

  private def initialize(): Unit = {
    mClient = LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    val schemaFieldPos: Map[String, Int] = schemaFieldNames.zipWithIndex.toMap
    try {
      new InterruptibleIterator[InternalRow](context, new NextIterator[InternalRow]() {
        val (startOffset, endOffset) = resolveRange(shardPartition)
        val startCursor: GetCursorResponse =
          mClient.GetCursor(logProject, logStore, shardPartition.shardId, startOffset)
        val endCursor: GetCursorResponse =
          mClient.GetCursor(logProject, logStore, shardPartition.shardId, endOffset)
        private var hasRead: Int = 0
        private val nextCursor: GetCursorResponse = startCursor
        private var nextCursorTime = nextCursor.GetCursor()
        private var nextCursorNano: Long =
          new String(Base64.getDecoder.decode(nextCursorTime)).toLong
        private val endCursorNano: Long =
          new String(Base64.getDecoder.decode(endCursor.GetCursor())).toLong
        // TODO: This may cost too much memory.
        private var logData = new LinkedBlockingQueue[LoghubData](4096 * step)
        private val inputMetrics = context.taskMetrics.inputMetrics

        private val schema = StructType.fromDDL(shardPartition.schemaDDL)
        private val valueConverters =
          schema.map(f => Utils.makeConverter(f.name, f.dataType, f.nullable)).toArray

        context.addTaskCompletionListener {
          _ => closeIfNeeded()
        }

        fetchNextBatch()

        def checkHasNext(): Boolean = nextCursorNano < endCursorNano || logData.nonEmpty

        override protected def getNext(): InternalRow = {
          finished = !checkHasNext()
          if (!finished) {
            if (logData.isEmpty) {
              fetchNextBatch()
            }
            if (logData.isEmpty) {
              finished = true
              null.asInstanceOf[InternalRow]
            } else {
              hasRead += 1
              val data = logData.poll()
              val row = new GenericInternalRow(schema.length)
              data.toArray.zipWithIndex.foreach { case (t, idx) =>
                row(idx) = valueConverters(idx).apply(t)
              }
              row.asInstanceOf[InternalRow]
            }
          } else {
            null.asInstanceOf[InternalRow]
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
          val batchGetLogRes: BatchGetLogResponse = mClient.BatchGetLog(logProject, logStore,
            shardPartition.shardId, step, nextCursorTime, endCursor.GetCursor())
          var count = 0
          var logGroupIndex = Utils.decodeCursorToTimestamp(nextCursorTime)
          batchGetLogRes.GetLogGroups().foreach(group => {
            val fastLogGroup = group.GetFastLogGroup()
            var logIndex = 0
            val logCount = fastLogGroup.getLogsCount
            for (i <- 0 until logCount) {
              val log = fastLogGroup.getLogs(i)
              val topic = fastLogGroup.getTopic
              val source = fastLogGroup.getSource
              try {
                if (defaultSchema) {
                  val obj = new JSONObject()
                  val fieldCount = log.getContentsCount
                  for (j <- 0 until fieldCount) {
                    val f = log.getContents(j)
                    obj.put(f.getKey, f.getValue)
                  }
                  for (i <- 0 until fastLogGroup.getLogTagsCount) {
                    val tag = fastLogGroup.getLogTags(i)
                    obj.put("__tag__:".concat(tag.getKey), tag.getValue)
                  }
                  if (appendSequenceNumber) {
                    obj.put(__SEQUENCE_NUMBER__, logGroupIndex + "-" + logIndex)
                  }
                  logData.offer(
                    new RawLoghubData(
                      logProject,
                      logStore,
                      shardPartition.shardId,
                      new java.sql.Timestamp(log.getTime * 1000L),
                      topic,
                      source,
                      obj.toJSONString))
                } else {
                  val columnArray = Array.tabulate(schemaFieldNames.length)(_ =>
                    (null, null).asInstanceOf[(String, String)]
                  )
                  for (i <- 0 until log.getContentsCount) {
                    val field = log.getContents(i)
                    if (schemaFieldPos.contains(field.getKey)) {
                      columnArray(schemaFieldPos(field.getKey)) = (field.getKey, field.getValue)
                    }
                  }
                  for (i <- 0 until fastLogGroup.getLogTagsCount) {
                    val tag = fastLogGroup.getLogTags(i)
                    val tagKey = tag.getKey
                    val tagValue = tag.getValue
                    if (schemaFieldPos.contains(s"__tag__:$tagKey")) {
                      columnArray(schemaFieldPos(s"__tag__:$tagKey")) =
                        (s"__tag__:$tagKey", tagValue)
                    }
                  }
                  if (schemaFieldPos.contains(__SEQUENCE_NUMBER__)) {
                    columnArray(schemaFieldPos(__SEQUENCE_NUMBER__)) =
                      (__SEQUENCE_NUMBER__, logGroupIndex + "-" + logIndex)
                  }
                  if (schemaFieldPos.contains(__PROJECT__)) {
                    columnArray(schemaFieldPos(__PROJECT__)) = (__PROJECT__, logProject)
                  }
                  if (schemaFieldPos.contains(__STORE__)) {
                    columnArray(schemaFieldPos(__STORE__)) = (__STORE__, logStore)
                  }
                  if (schemaFieldPos.contains(__SHARD__)) {
                    columnArray(schemaFieldPos(__SHARD__)) =
                      (__SHARD__, shardPartition.shardId.toString)
                  }
                  if (schemaFieldPos.contains(__TOPIC__)) {
                    columnArray(schemaFieldPos(__TOPIC__)) = (__TOPIC__, topic)
                  }
                  if (schemaFieldPos.contains(__SOURCE__)) {
                    columnArray(schemaFieldPos(__SOURCE__)) = (__SOURCE__, source)
                  }
                  if (schemaFieldPos.contains(__TIME__)) {
                    columnArray(schemaFieldPos(__TIME__)) =
                      (__TIME__, new java.sql.Timestamp(log.getTime * 1000L).toString)
                  }

                  count += 1
                  logData.offer(new SchemaLoghubData(columnArray))
                }
              } catch {
                case e: NoSuchElementException =>
                  logWarning(s"Meet an unknown column name, ${e.getMessage}. Treat this as " +
                    "an invalid data and continue.")
              }
              logIndex += 1
            }
            logGroupIndex += 1
          })

          val crt = nextCursorTime
          nextCursorTime = batchGetLogRes.GetNextCursor()
          nextCursorNano = new String(Base64.getDecoder.decode(nextCursorTime)).toLong
          logDebug(s"project: $logProject, logStore: $logStore, " +
            s"shardId: ${shardPartition.shardId}, " +
            s"startCursor: $startOffset, endCursor: $endOffset, " +
            s"currentCursorTime: $crt, nextCursorTime: $nextCursorTime, " +
            s"nextCursorNano: $nextCursorNano, endCursorNano: $endCursorNano, " +
            s"hasRead: $hasRead, batchGet: $count, queueSize: ${logData.size()}")
        }
      })
    } catch {
      case _: Exception =>
        Iterator.empty.asInstanceOf[Iterator[InternalRow]]
    }
  }

  private def resolveRange(shardPartition: ShardPartition): (Int, Int) = {
    val loghubOffsetReader = new LoghubOffsetReader(Map(
      "sls.project" -> logProject,
      "sls.store" -> logStore,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "endpoint" -> endpoint
    ))
    // Obtain TopicPartition offsets with late binding support
    lazy val earliest = loghubOffsetReader.fetchEarliestOffsets()
    lazy val latest = loghubOffsetReader.fetchLatestOffsets()
    val startOffset = if (shardPartition.startCursor < 0) {
      assert(shardPartition.startCursor == LoghubOffsetRangeLimit.EARLIEST,
        s"earliest offset ${shardPartition.startCursor} does not equal " +
          s"${LoghubOffsetRangeLimit.EARLIEST}")
      earliest(LoghubShard(logProject, logStore, shardPartition.shardId))._1
    } else {
      shardPartition.startCursor
    }

    val endOffset = if (shardPartition.endCursor < 0) {
      assert(shardPartition.endCursor == LoghubOffsetRangeLimit.LATEST,
        s"earliest offset ${shardPartition.endCursor} does not equal " +
          s"${LoghubOffsetRangeLimit.LATEST}")
      latest(LoghubShard(logProject, logStore, shardPartition.shardId))._1
    } else {
      shardPartition.endCursor
    }

    (startOffset, endOffset)
  }

  override protected def getPartitions: Array[Partition] = {
    val logGroupStep = sc.getConf.get("spark.loghub.batchGet.step", "100").toInt
    shardOffsets.zipWithIndex.map { case (p, idx) =>
      new ShardPartition(id, idx, p._1, logProject, logStore, accessKeyId, accessKeySecret,
        endpoint, schemaDDL, p._2, p._3, logGroupStep).asInstanceOf[Partition]
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
      val schemaDDL: String,
      val startCursor: Int,
      val endCursor: Int,
      val logGroupStep: Int = 100) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId
    override def equals(other: Any): Boolean = super.equals(other)
    override def index: Int = partitionId
  }
}
