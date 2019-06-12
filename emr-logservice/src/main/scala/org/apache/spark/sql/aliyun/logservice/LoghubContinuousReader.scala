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

import scala.collection.JavaConversions._
import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.response.BatchGetLogResponse

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, SupportsScanUnsafeRow}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class LoghubContinuousReader(
    schema: Option[StructType],
    offsetReader: LoghubOffsetReader,
    loghubParams: util.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    initialOffsets: LoghubOffsetRangeLimit)
  extends ContinuousReader with SupportsScanUnsafeRow with Logging {

  private lazy val session = SparkSession.getActiveSession.get
  private lazy val sc = session.sparkContext

  private val pollTimeoutMs = sourceOptions.getOrElse("loghub.pollTimeoutMs", "512").toLong

  private var offset: Offset = _

  override def readSchema: StructType = Utils.getSchema(schema, sourceOptions)

  override def commit(end: Offset): Unit = {}

  override def deserializeOffset(json: String): Offset = {
    LoghubSourceOffset(LoghubSourceOffset.partitionOffsets(json))
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
      case LoghubShardOffset(lp, ls, shard, of) => Map(LoghubShard(lp, ls, shard) -> of)
    }.reduce(_ ++ _)
    LoghubSourceOffset(mergedMap)
  }

  override def createUnsafeRowReaderFactories(): util.List[DataReaderFactory[UnsafeRow]] = {
    import scala.collection.JavaConverters._
    val startOffsets = LoghubSourceOffset.getShardOffsets(offset)
    startOffsets.toSeq.map {
      case (loghubShard, of) => {
        LoghubContinuousDataReaderFactory(loghubShard.logProject, loghubShard.logStore, loghubShard.shard, of, sourceOptions)
          .asInstanceOf[DataReaderFactory[UnsafeRow]]
      }
    }.asJava
  }

  override def stop(): Unit = offsetReader.close()

  override def toString(): String = s"LoghubSource[$offsetReader]"
}

case class LoghubContinuousDataReaderFactory(
    logProject: String,
    logStore: String,
    shardId: Int,
    offset: String,
    sourceOptions: Map[String, String]) extends DataReaderFactory[UnsafeRow] {
  override def createDataReader(): LoghubContinuousDataReader = {
    new LoghubContinuousDataReader(logProject, logStore, shardId, offset, sourceOptions)
  }
}

class LoghubContinuousDataReader(
    logProject: String,
    logStore: String,
    shardId: Int,
    offset: String,
    sourceOptions: Map[String, String]) extends ContinuousDataReader[UnsafeRow] with Logging {

  private var logServiceClient = LoghubOffsetReader.getOrCreateLoghubClient(sourceOptions)

  private val step: Int = 2
  private var hasRead: Int = 0
  private var nextCursor: String = offset
  private var endCursor = logServiceClient.GetCursor(logProject, logStore, shardId, CursorMode.END).GetCursor()
  // TODO: This may cost too much memory.
  private val logData = new LinkedBlockingQueue[JSONObject](4096 * step)

  private val sharedRow = new UnsafeRow(7)
  private val bufferHolder = new BufferHolder(sharedRow)
  private val rowWriter = new UnsafeRowWriter(bufferHolder, 6)

  private var currentRecord: JSONObject = _

  override def getOffset: PartitionOffset = LoghubShardOffset(logProject, logStore, shardId, nextCursor)

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
    endCursor = logServiceClient.GetCursor(logProject, logStore, shardId, CursorMode.END).GetCursor()
    val batchGetLogRes: BatchGetLogResponse = logServiceClient.BatchGetLog(logProject, logStore, shardId,
      step, nextCursor, endCursor)
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
        logData.offer(obj)
      })
    })

    val crt = nextCursor
    nextCursor = batchGetLogRes.GetNextCursor()
    logDebug(s"shardId: $shardId, currentCursor: $crt, nextCursor: $nextCursor," +
      s" endCursor: $endCursor, hasRead: $hasRead, count in this batch: $count," +
      s" get: $count, queue: ${logData.size()}")
  }

  override def get(): UnsafeRow = {
    bufferHolder.reset()

    rowWriter.write(0, UTF8String.fromString(logProject))
    rowWriter.write(1, UTF8String.fromString(logStore))
    rowWriter.write(2, shardId)
    rowWriter.write(3, DateTimeUtils.fromJavaTimestamp(
      new java.sql.Timestamp(currentRecord.get(__TIME__).asInstanceOf[Integer] * 1000)))
    rowWriter.write(4, currentRecord.toJSONString.getBytes)
    sharedRow.setTotalSize(bufferHolder.totalSize)
    sharedRow
  }

  override def close(): Unit = {
    logData.clear()
    logServiceClient = null
  }
}