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

import java.util.Locale

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aliyun.datahub.DatahubOffsetRangeLimit._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.NextIterator

class DatahubSourceRDD(
    @transient sc: SparkContext,
    schema: StructType,
    parameters: Map[String, String]) extends RDD[InternalRow](sc, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val sourcePartition = split.asInstanceOf[DatahubInputPartition]
    val newRange = resolveOffset(sourcePartition.offsetRange)
    val partitionReader = DatahubMicroBatchInputPartitionReader(
      newRange, false, parameters, sourcePartition.schemaDdl)

    if (newRange.fromOffset >= newRange.untilOffset) {
      logInfo(s"Beginning offset ${newRange.fromOffset} is larger than " +
        s"ending offset ${newRange.untilOffset}, skip ${newRange.datahubShard}")
      Iterator.empty
    } else {
      val underlying = new NextIterator[InternalRow]() {
        override def getNext(): InternalRow = {
          if (partitionReader.next()) {
            partitionReader.get()
          } else {
            finished = true
            null
          }
        }

        override protected def close(): Unit = {
          partitionReader.close()
        }
      }
      // Release consumer, either by removing it or indicating we're no longer using it
      context.addTaskCompletionListener[Unit] { _ =>
        underlying.closeIfNeeded()
      }
      underlying
    }
  }

  override protected def getPartitions: Array[Partition] = {
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
    offsetRanges.zipWithIndex.map { case (range, idx) =>
      DatahubInputPartition(idx, range, parameters, schema.toDDL): Partition
    }.toArray
  }

  private def resolveOffset(range: DatahubOffsetRange): DatahubOffsetRange = {
    var datahubOffsetReader: DatahubOffsetReader = null
    try {
      datahubOffsetReader = new DatahubOffsetReader(parameters)
      val latestOffsets = datahubOffsetReader.fetchLatestOffsets()
      val earliestOffets = datahubOffsetReader.fetchEarliestOffsets()
      if (range.fromOffset < 0 || range.untilOffset < 0) {
        val fromOffset = if (range.fromOffset < 0) {
          assert(range.fromOffset == DatahubOffsetRangeLimit.OLDEST,
            s"earliest offset ${range.fromOffset} does not equal ${DatahubOffsetRangeLimit.OLDEST}")
          earliestOffets(range.datahubShard)
        } else {
          range.fromOffset
        }
        val untilOffset = if (range.untilOffset < 0) {
          assert(range.untilOffset == DatahubOffsetRangeLimit.LATEST,
            s"latest offset ${range.untilOffset} does not equal ${DatahubOffsetRangeLimit.LATEST}")
          latestOffsets(range.datahubShard)
        } else {
          range.untilOffset
        }
        // DatahubMicroBatchInputPartitionReader read datahub with [start, end), so increase
        // the end offset + 1 to read the last data.
        DatahubOffsetRange(range.datahubShard, fromOffset, untilOffset + 1)
      } else {
        // DatahubMicroBatchInputPartitionReader read datahub with [start, end), so increase
        // the end offset + 1 to read the last data.
        DatahubOffsetRange(range.datahubShard, range.fromOffset, range.untilOffset + 1)
      }
    } finally {
      if (datahubOffsetReader != null) {
        datahubOffsetReader.close()
        datahubOffsetReader = null
      }
    }
  }
}

private case class DatahubInputPartition(
    index: Int,
    offsetRange: DatahubOffsetRange,
    sourceOptions: Map[String, String],
    schemaDdl: String) extends Partition
