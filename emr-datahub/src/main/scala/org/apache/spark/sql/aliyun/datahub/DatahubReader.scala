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
import java.util.Locale
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._

import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.model.RecordEntry

import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.datahub.DatahubOffsetRangeLimit._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

class DatahubReader(
    schema: StructType,
    sourceOptions: DataSourceOptions) extends DataSourceReader with Logging {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val parameters = sourceOptions.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    val startingRelationOffsets = DatahubOffsetRangeLimit.getOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, OldestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = DatahubOffsetRangeLimit.getOffsetRangeLimit(
      caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    assert(endingRelationOffsets != OldestOffsetRangeLimit)

    def validateTopicPartitions(
        shards: Set[DatahubShard],
        shardOffsets: Map[DatahubShard, Long]): Unit = {
      assert(shards == shardOffsets.keySet, "If startingOffsets contains specific offsets, " +
        "you must specify all shard.\nUse -1 for latest, -2 for oldest, if you don't care.\n" +
        s"Specified: ${shardOffsets.keySet} Assigned: $shards")
      logDebug(s"Shards assigned to consumer: $shards. Seeking to $shardOffsets")
    }

    val datahubOffsetReader = new DatahubOffsetReader(caseInsensitiveParams)
    val shards = datahubOffsetReader.fetchDatahubShard()
    val startPartitionOffsets = startingRelationOffsets match {
      case OldestOffsetRangeLimit =>
        shards.map { tp => tp -> OLDEST }.toMap
      case SpecificOffsetRangeLimit(shardOffsets) =>
        validateTopicPartitions(shards, shardOffsets)
        shardOffsets.map(so => (so._1, so._2))
    }

    val endPartitionOffsets = endingRelationOffsets match {
      case LatestOffsetRangeLimit =>
        shards.map { tp => tp -> LATEST }.toMap
      case SpecificOffsetRangeLimit(shardOffsets) =>
        validateTopicPartitions(shards, shardOffsets)
        shardOffsets.map(so => (so._1, so._2))
    }

    require(startPartitionOffsets.size == endPartitionOffsets.size)
    val offsetRanges = startPartitionOffsets.toSeq.sortBy(_._1.shardId)
      .zip(endPartitionOffsets.toSeq.sortBy(_._1.shardId)).map { case (start, end) =>
      require(start._1.shardId == end._1.shardId)
      DatahubOffsetRange(start._1, start._2, end._2)
    }.filter(_.size > 0)

    // Generate factories based on the offset ranges
    offsetRanges.map { range =>
      DatahubInputPartition(range,
        sourceOptions.asMap().asScala.toMap, readSchema().toDDL): InputPartition[InternalRow]
    }.asJava
  }

  private case class DatahubInputPartition(
      offsetRange: DatahubOffsetRange,
      sourceOptions: Map[String, String],
      schemaDdl: String)
      extends InputPartition[InternalRow] {

    override def preferredLocations(): Array[String] = Array.empty[String]

    override def createPartitionReader(): InputPartitionReader[InternalRow] =
      DatahubInputPartitionReader(offsetRange, sourceOptions, schemaDdl)
  }

  private case class DatahubInputPartitionReader(
      offsetRange: DatahubOffsetRange,
      sourceOptions: Map[String, String],
      schemaDdl: String)
      extends InputPartitionReader[InternalRow] with Logging {

    private val accessKeyId = sourceOptions("access.key.id")
    private val accessKeySecret = sourceOptions("access.key.secret")
    private val endpoint = sourceOptions("endpoint")
    private val project = offsetRange.datahubShard.project
    private val topic = offsetRange.datahubShard.topic
    private val shardId = offsetRange.datahubShard.shardId
    @transient private val datahubClientAgent =
      DatahubOffsetReader.getOrCreateDatahubClient(accessKeyId, accessKeySecret, endpoint)

    private val step = 100
    private var hasRead = 0
    private val dataBuffer = new LinkedBlockingQueue[RecordEntry](step)

    @transient private val schema: StructType = StructType.fromDDL(schemaDdl)
    private val converter: DatahubRecordToUnsafeRowConverter =
      new DatahubRecordToUnsafeRowConverter(schema, sourceOptions)
    private val (start, end) = resolveRange()
    private var nextOffset = start
    private var nextCursor: String =
      datahubClientAgent.getCursor(project, topic, shardId, nextOffset).getCursor
    private var nextRow: UnsafeRow = _

    override def next(): Boolean = {
      // read range: [start, end)
      if (nextOffset < end) {
        if (dataBuffer.isEmpty) {
          fetchData()
        }

        if (dataBuffer.isEmpty) {
          false
        } else {
          val record = dataBuffer.poll()
          nextRow = converter.toUnsafeRow(record, project, topic, shardId)
          nextOffset += 1
          true
        }
      } else {
        false
      }
    }

    private def fetchData(): Unit = {
      val topicResult = datahubClientAgent.getTopic(project, topic)
      val recordSchema = topicResult.getRecordSchema
      val limit = if (end - nextOffset >= step) {
        step
      } else {
        end - nextOffset
      }
      val recordResult = datahubClientAgent
        .getRecords(project, topic, shardId, nextCursor, limit.toInt, recordSchema)
      recordResult.getRecords.asScala.foreach(record => {
        dataBuffer.offer(record)
      })
      nextCursor = recordResult.getNextCursor
      hasRead = hasRead + recordResult.getRecordCount
      logDebug(s"shardId: $shardId, nextCursor: $nextCursor, hasRead: $hasRead, " +
        s"total: ${end - start}")

    }

    override def get(): UnsafeRow = {
      assert(nextRow != null)
      nextRow
    }

    override def close(): Unit = {
      datahubClientAgent.close()
    }

    private def resolveRange() = {
      val earliestOffset =
        datahubClientAgent.getCursor(project, topic, shardId, CursorType.OLDEST).getSequence
      val latestOffset =
        datahubClientAgent.getCursor(project, topic, shardId, CursorType.LATEST).getSequence
      if (offsetRange.fromOffset < 0 || offsetRange.untilOffset < 0) {
        val fromOffset = if (offsetRange.fromOffset < 0) {
          assert(offsetRange.fromOffset == DatahubOffsetRangeLimit.OLDEST,
            s"earliest offset ${offsetRange.fromOffset} does not equal " +
            s"${DatahubOffsetRangeLimit.OLDEST}")
          earliestOffset
        } else {
          offsetRange.fromOffset
        }
        val untilOffset = if (offsetRange.untilOffset < 0) {
          assert(offsetRange.untilOffset == DatahubOffsetRangeLimit.LATEST,
            s"latest offset ${offsetRange.untilOffset} does not equal " +
            s"${DatahubOffsetRangeLimit.LATEST}")
          latestOffset
        } else {
          offsetRange.untilOffset
        }
        (fromOffset, untilOffset)
      } else {
        (offsetRange.fromOffset, offsetRange.untilOffset)
      }
    }
  }
}
