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

import java.util
import java.util.Optional
import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.response.BatchGetLogResponse

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.sources.v2.reader.{ContinuousInputPartition, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class LoghubContinuousReader(
    schema: StructType,
    defaultSchema: Boolean,
    offsetReader: LoghubOffsetReader,
    loghubParams: util.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    initialOffsets: LoghubOffsetRangeLimit)
  extends ContinuousReader with Logging {

  private var offset: Offset = _

  override def readSchema: StructType = schema

  override def commit(end: Offset): Unit = {}

  override def deserializeOffset(json: String): Offset = {
    LoghubSourceOffset(LoghubSourceOffset.partitionOffsets(json, sourceOptions))
  }

  override def getStartOffset: Offset = offset

  override def setStartOffset(start: Optional[Offset]): Unit = {
    offset = start.orElse {
      val offsets = initialOffsets match {
        case EarliestOffsetRangeLimit => LoghubSourceOffset(offsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => LoghubSourceOffset(offsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(_) =>
          throw new UnsupportedOperationException("Does not support SpecificOffsetRangeLimit.")
      }
      logInfo(s"Initial offsets: $offsets")
      offsets
    }
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    val mergedMap = offsets.map {
      case LoghubShardOffset(lp, ls, shard, of, cursor) =>
        Map(LoghubShard(lp, ls, shard) -> (of, cursor))
    }.reduce(_ ++ _)
    LoghubSourceOffset(mergedMap)
  }

  override def stop(): Unit = offsetReader.close()

  override def toString(): String = s"LoghubSource[$offsetReader]"

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._
    val startOffsets = LoghubSourceOffset.getShardOffsets(offset, sourceOptions)
    startOffsets.toSeq.map { case (loghubShard, of) =>
      LoghubContinuousInputPartition(
        loghubShard.logProject,
        loghubShard.logStore,
        loghubShard.shard,
        of._1,
        sourceOptions,
        readSchema.fieldNames,
        defaultSchema): InputPartition[InternalRow]
    }.asJava
  }
}

case class LoghubContinuousInputPartition(
    logProject: String,
    logStore: String,
    shardId: Int,
    offset: Int,
    sourceOptions: Map[String, String],
    schemaFieldNames: Array[String],
    defaultSchema: Boolean) extends ContinuousInputPartition[InternalRow] {
  override def createContinuousReader(offset: PartitionOffset):
    InputPartitionReader[InternalRow] = {
    val off = offset.asInstanceOf[LoghubShardOffset]
    new LoghubContinuousInputPartitionReader(
      logProject,
      logStore,
      shardId,
      off.offset,
      sourceOptions,
      schemaFieldNames,
      defaultSchema)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new LoghubContinuousInputPartitionReader(
      logProject,
      logStore,
      shardId,
      offset,
      sourceOptions,
      schemaFieldNames,
      defaultSchema)
  }
}

class LoghubContinuousInputPartitionReader(
    logProject: String,
    logStore: String,
    shardId: Int,
    offset: Int,
    sourceOptions: Map[String, String],
    schemaFieldNames: Array[String],
    defaultSchema: Boolean) extends ContinuousInputPartitionReader[InternalRow] with Logging {

  private var logServiceClient = LoghubOffsetReader.getOrCreateLoghubClient(sourceOptions)

  private val step: Int = sourceOptions.getOrElse("loghub.batchGet.step", "100").toInt
  private val appendSequenceNumber: Boolean =
    sourceOptions.getOrElse("appendSequenceNumber", "false").toBoolean
  private var hasRead: Int = 0
  private var nextCursor: String =
    logServiceClient.GetCursor(logProject, logStore, shardId, offset).GetCursor()
  private var endCursor =
    logServiceClient.GetCursor(logProject, logStore, shardId, CursorMode.END).GetCursor()
  // TODO: This may cost too much memory.
  private val logData = new LinkedBlockingQueue[LoghubData](4096 * step)
  private val rowWriter = new UnsafeRowWriter(schemaFieldNames.length)

  private var currentRecord: LoghubData = _

  override def getOffset: PartitionOffset = {
    val offset = logServiceClient.GetCursorTime(logProject, logStore, shardId, nextCursor)
    LoghubShardOffset(logProject, logStore, shardId, offset.GetCursorTime(), nextCursor)
  }

  override def next(): Boolean = {
    if (TaskContext.get().isInterrupted() || TaskContext.get().isCompleted()) {
      return false
    }
    while (logData.isEmpty) {
      fetchNextBatch()
    }
    hasRead += 1
    currentRecord = logData.poll()
    true
  }

  def fetchNextBatch(): Unit = {
    // scalastyle:off
    import scala.collection.JavaConversions._
    // scalastyle:on
    endCursor =
      logServiceClient.GetCursor(logProject, logStore, shardId, CursorMode.END).GetCursor()
    val batchGetLogRes: BatchGetLogResponse =
      logServiceClient.BatchGetLog(logProject, logStore, shardId, step, nextCursor, endCursor)
    val schemaFieldPos: Map[String, Int] = schemaFieldNames.zipWithIndex.toMap
    var count = 0
    var logGroupIndex = Utils.decodeCursorToTimestamp(nextCursor)
    batchGetLogRes.GetLogGroups().foreach(group => {
      val logGroup = group.GetFastLogGroup()
      val logCount = logGroup.getLogsCount
      var logIndex = 0
      for (i <- 0 until logCount) {
        val log = logGroup.getLogs(i)
        val topic = logGroup.getTopic
        val source = logGroup.getSource
        if (defaultSchema) {
          val obj = new JSONObject()
          for (i <- 0 until log.getContentsCount) {
            val field = log.getContents(i)
            obj.put(field.getKey, field.getValue)
          }
          for (i <- 0 until logGroup.getLogTagsCount) {
            val tag = logGroup.getLogTags(i)
            obj.put("__tag__:".concat(tag.getKey), tag.getValue)
          }
          if (appendSequenceNumber) {
            obj.put(__SEQUENCE_NUMBER__, logGroupIndex + "-" + logIndex)
          }

          logData.offer(
            new RawLoghubData(
              logProject,
              logStore,
              shardId,
              new java.sql.Timestamp(log.getTime * 1000L),
              topic,
              source,
              obj.toJSONString))
        } else {
          try {
            val columnArray = Array.tabulate(schemaFieldNames.length)(_ =>
              (null, null).asInstanceOf[(String, String)]
            )
            for (i <- 0 until log.getContentsCount) {
              val field = log.getContents(i)
              if (schemaFieldPos.contains(field.getKey)) {
                columnArray(schemaFieldPos(field.getKey)) = (field.getKey, field.getValue)
              }
            }
            for (i <- 0 until logGroup.getLogTagsCount) {
              val tag = logGroup.getLogTags(i)
              val tagKey = tag.getKey
              val tagValue = tag.getValue
              if (schemaFieldPos.contains(tagKey)) {
                columnArray(schemaFieldPos(tagKey)) = (tagKey, tagValue)
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
              columnArray(schemaFieldPos(__SHARD__)) = (__SHARD__, shardId.toString)
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
          } catch {
            case e: NoSuchElementException =>
              logWarning(s"Meet an unknown column name, ${e.getMessage}. Treat this as " +
                "an invalid data and continue.")
          }
          logIndex += 1
        }
      }
      logGroupIndex += 1
    })

    val crt = nextCursor
    nextCursor = batchGetLogRes.GetNextCursor()
    logDebug(s"shardId: $shardId, currentCursor: $crt, nextCursor: $nextCursor," +
      s" endCursor: $endCursor, hasRead: $hasRead, count in this batch: $count," +
      s" get: $count, queue: ${logData.size()}")
  }

  override def get(): UnsafeRow = {
    rowWriter.reset()
    rowWriter.zeroOutNullBytes()

    currentRecord.toArray.zipWithIndex.foreach(item => {
      rowWriter.write(item._2, UTF8String.fromString(item._1.asInstanceOf[String]))
    })

    rowWriter.getRow
  }

  override def close(): Unit = {
    logData.clear()
    logServiceClient = null
  }
}