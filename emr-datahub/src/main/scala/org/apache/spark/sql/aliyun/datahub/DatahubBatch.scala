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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.datahub.DatahubOffsetRangeLimit._
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DatahubBatch(
    @transient offsetReader: DatahubOffsetReader,
    @transient sourceOptions: CaseInsensitiveStringMap,
    startingOffsets: DatahubOffsetRangeLimit,
    endingOffsets: DatahubOffsetRangeLimit,
    failOnDataLoss: Boolean,
    userSpecifiedSchemaDdl: Option[String]) extends Batch with Logging {

  private val userSpecifiedSchema =
    if (userSpecifiedSchemaDdl.isDefined && userSpecifiedSchemaDdl.get.nonEmpty) {
      userSpecifiedSchemaDdl.map(StructType.fromDDL)
    } else {
      Some(new StructType())
    }

  private def readSchema(): StructType = {
    DatahubSchema.getSchema(userSpecifiedSchema, sourceOptions.asScala.toMap)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val shards = offsetReader.fetchDatahubShard()

    def validateTopicPartitions(
        shards: Set[DatahubShard],
        shardOffsets: Map[DatahubShard, Long]): Unit = {
      assert(shards == shardOffsets.keySet, "If startingOffsets contains specific offsets, " +
        "you must specify all shard.\nUse -1 for latest, -2 for oldest, if you don't care.\n" +
        s"Specified: ${shardOffsets.keySet} Assigned: $shards")
      logDebug(s"Shards assigned to consumer: $shards. Seeking to $shardOffsets")
    }

    val startPartitionOffsets = startingOffsets match {
      case OldestOffsetRangeLimit =>
        shards.map { tp => tp -> OLDEST }.toMap
      case SpecificOffsetRangeLimit(shardOffsets) =>
        validateTopicPartitions(shards, shardOffsets)
        shardOffsets.map(so => (so._1, so._2))
    }

    val endPartitionOffsets = endingOffsets match {
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

    offsetRanges.map { range =>
      DatahubBatchInputPartition(range, failOnDataLoss,
        sourceOptions.asScala.toMap, readSchema().toDDL)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    DatahubBatchReaderFactory
  }

  override def toString: String =
    s"DatahubBatch(start=$startingOffsets, end=$endingOffsets)"
}
