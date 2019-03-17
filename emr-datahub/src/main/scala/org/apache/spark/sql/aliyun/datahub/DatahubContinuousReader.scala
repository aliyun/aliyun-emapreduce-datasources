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
package org.apache.spark.sql.aliyun.datahub

import java.util
import java.util.Optional
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConversions._

import com.alibaba.fastjson.JSONObject
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, SupportsScanUnsafeRow}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DatahubContinuousReader(
    schema: Option[StructType],
    offsetReader: DatahubOffsetReader,
    datahubParams: util.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    initialOffsets: DatahubOffsetRangeLimit)
  extends ContinuousReader with SupportsScanUnsafeRow with Logging {

  private lazy val session = SparkSession.getActiveSession.get
  private lazy val sc = session.sparkContext

  private var offset: Offset = _

  override def readSchema: StructType = DatahubSchema.getSchema(schema, sourceOptions)

  override def commit(end: Offset): Unit = {}

  override def deserializeOffset(json: String): Offset = {
    DatahubSourceOffset(DatahubSourceOffset.partitionOffsets(json))
  }

  override def getStartOffset: Offset = offset

  override def setStartOffset(start: Optional[Offset]): Unit = {
    offset = start.orElse {
      val offsets = initialOffsets match {
        case OldestOffsetRangeLimit => DatahubSourceOffset(offsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => DatahubSourceOffset(offsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(_) =>
          throw new UnsupportedOperationException("Does not support SpecificOffsetRangeLimit.")
      }
      logInfo(s"Initial offsets: $offsets")
      offsets
    }
  }

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    val mergedMap = offsets.map {
      case DatahubShardOffset(lp, ls, shard, of) => Map(DatahubShard(lp, ls, shard) -> of)
    }.reduce(_ ++ _)
    DatahubSourceOffset(mergedMap)
  }

  override def createUnsafeRowReaderFactories(): util.List[DataReaderFactory[UnsafeRow]] = {
    import scala.collection.JavaConverters._
    val startOffsets = DatahubSourceOffset.getShardOffsets(offset)
    startOffsets.toSeq.map {
      case (datahubShard, of) => {
        DatahubContinuousDataReaderFactory(datahubShard.project, datahubShard.topic, datahubShard.shardId, of, sourceOptions)
          .asInstanceOf[DataReaderFactory[UnsafeRow]]
      }
    }.asJava
  }

  override def stop(): Unit = offsetReader.close()

  override def toString(): String = s"DatahubSource[$offsetReader]"
}

case class DatahubContinuousDataReaderFactory(
    project: String,
    topic: String,
    shardId: String,
    offset: Long,
    sourceOptions: Map[String, String]) extends DataReaderFactory[UnsafeRow] {
  override def createDataReader(): DatahubContinuousDataReader = {
    new DatahubContinuousDataReader(project, topic, shardId, offset, sourceOptions)
  }
}

class DatahubContinuousDataReader(
    project: String,
    topic: String,
    shardId: String,
    offset: Long,
    sourceOptions: Map[String, String]) extends ContinuousDataReader[UnsafeRow] with Logging {

  private var datahubClient = DatahubOffsetReader.getOrCreateDatahubClient(sourceOptions)

  private val step: Int = 100
  private var hasRead: Int = 0
  private var nextOffset = offset
  private var endOffset = datahubClient.getCursor(project, topic, shardId, CursorType.LATEST).getSequence()
  private val dataBuffer = new LinkedBlockingQueue[JSONObject](step)
  private val sharedRow = new UnsafeRow(7)
  private val bufferHolder = new BufferHolder(sharedRow)
  private val rowWriter = new UnsafeRowWriter(bufferHolder, 6)
  private var currentRecord: JSONObject = _

  override def getOffset: PartitionOffset = DatahubShardOffset(project, topic, shardId, nextOffset)

  override def next(): Boolean = {
    if (TaskContext.get().isInterrupted() || TaskContext.get().isCompleted()) {
      return false
    }
    while (dataBuffer.isEmpty) {
      fetchNextBatch()
    }
    hasRead += 1
    currentRecord = dataBuffer.poll()
    true
  }

  def fetchNextBatch(): Unit = {
    endOffset = datahubClient.getCursor(project, topic, shardId, CursorType.LATEST).getSequence
    val topicResult = datahubClient.getTopic(project, topic)
    val schema = topicResult.getRecordSchema
    val limit = endOffset - nextOffset
    val cursor = datahubClient.getCursor(project, topic, shardId, nextOffset).getCursor
    val recordResult = datahubClient.getRecords(project, topic, shardId, cursor, limit.toInt, schema)
    val num = recordResult.getRecordCount
    recordResult.getRecords.foreach(record => {
      record.getSystemTime
      val obj = new JSONObject()
      obj.put(DatahubSchema.SYSTEM_TIME, record.getSystemTime.toInt)
      obj.put(DatahubSchema.TOPIC, topic)
      obj.put(DatahubSchema.PROJECT, project)
      record.getFields.foreach(field => {
        obj.put(field.getName, record.get(field.getName))
      })

      dataBuffer.offer(obj)
      nextOffset = record.getOffset.getSequence
    })
    hasRead = hasRead + num
    logDebug(s"shardId: $shardId, nextCursor: $nextOffset, hasRead: $hasRead")
  }

  override def get(): UnsafeRow = {
    bufferHolder.reset()
    rowWriter.write(0, UTF8String.fromString(project))
    rowWriter.write(1, UTF8String.fromString(topic))
    rowWriter.write(2, UTF8String.fromString(shardId))
    rowWriter.write(3, DateTimeUtils.fromJavaTimestamp(
      new java.sql.Timestamp(currentRecord.get(DatahubSchema.SYSTEM_TIME).asInstanceOf[Long])))
    rowWriter.write(4, currentRecord.toJSONString.getBytes)
    sharedRow.setTotalSize(bufferHolder.totalSize)
    sharedRow
  }

  override def close(): Unit = {
    dataBuffer.clear()
    datahubClient = null
  }
}