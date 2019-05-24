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
import com.aliyun.datahub.common.data.Field
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DatahubContinuousReader(
    schema: Option[StructType],
    offsetReader: DatahubOffsetReader,
    datahubParams: util.Map[String, Object],
    sourceOptions: Map[String, String],
    metadataPath: String,
    initialOffsets: DatahubOffsetRangeLimit)
  extends ContinuousReader with Logging {

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

  override def stop(): Unit = offsetReader.close()

  override def toString(): String = s"DatahubSource[$offsetReader]"

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._
    val startOffsets = DatahubSourceOffset.getShardOffsets(offset)
    startOffsets.toSeq.map {
      case (datahubShard, of) => {
        DatahubContinuousInputPartition(
          datahubShard.project, datahubShard.topic, datahubShard.shardId, of, sourceOptions)
        : InputPartition[InternalRow]
      }
    }.asJava
  }
}

case class DatahubContinuousInputPartition(
    project: String,
    topic: String,
    shardId: String,
    offset: Long,
    sourceOptions: Map[String, String]) extends ContinuousInputPartition[InternalRow] {
  override def createContinuousReader(offset: PartitionOffset): InputPartitionReader[InternalRow] = {
    val off = offset.asInstanceOf[DatahubShardOffset]
    new DatahubContinuousInputPartitionReader(project, topic, shardId, off.offset, sourceOptions)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new DatahubContinuousInputPartitionReader(project, topic, shardId, offset, sourceOptions)
  }
}

class DatahubContinuousInputPartitionReader(
    project: String,
    topic: String,
    shardId: String,
    offset: Long,
    sourceOptions: Map[String, String]) extends ContinuousInputPartitionReader[InternalRow] with Logging {

  private var datahubClient = DatahubOffsetReader.getOrCreateDatahubClient(sourceOptions)

  private val step: Int = 100
  private var hasRead: Int = 0
  private var nextOffset = offset
  private var endOffset = datahubClient.getCursor(project, topic, shardId, CursorType.LATEST).getSequence()
  private val dataBuffer = new LinkedBlockingQueue[RecordEntry](step)
  private var currentRecord: RecordEntry = null
  private val fields: List[Field] = datahubClient.getTopic(project, topic).getRecordSchema.getFields.toList
  private val rowWriter = new UnsafeRowWriter(4 + fields.length)

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
    if (limit > 0) {
      val cursor = datahubClient.getCursor(project, topic, shardId, nextOffset).getCursor
      val recordResult = datahubClient.getRecords(project, topic, shardId, cursor, limit.toInt, schema)
      val num = recordResult.getRecordCount
      recordResult.getRecords.foreach(record => {
        dataBuffer.offer(record)
        nextOffset = record.getOffset.getSequence
      })
      hasRead = hasRead + num
      logDebug(s"shardId: $shardId, nextCursor: $nextOffset, hasRead: $hasRead")
    }
  }

  override def get(): UnsafeRow = {
    rowWriter.reset()
    rowWriter.zeroOutNullBytes()

    rowWriter.write(0, UTF8String.fromString(project))
    rowWriter.write(1, UTF8String.fromString(topic))
    rowWriter.write(2, UTF8String.fromString(shardId))
    rowWriter.write(3, DateTimeUtils.fromJavaTimestamp(
      new java.sql.Timestamp(currentRecord.getSystemTime)))

    fields.zipWithIndex.foreach({
      case (f, index) =>
        val field = f.getName
        val value = currentRecord.getString(field)
        rowWriter.write(index + 4, UTF8String.fromString(value))
    })

    rowWriter.getRow
  }

  override def close(): Unit = {
    dataBuffer.clear()
    datahubClient = null
  }
}