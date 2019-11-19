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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => OffsetV2, PartitionOffset}

// scalastyle:off
case class DatahubSourceOffset(shardToOffsets: Map[DatahubShard, Long]) extends OffsetV2 {
  override def json(): String = DatahubSourceOffset.partitionOffsets(shardToOffsets)

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case _ @ DatahubSourceOffset(offsets) =>
        if (offsets.keySet != shardToOffsets.keySet) {
          false
        } else {
          var findUnEquals = false
          offsets.foreach { case (shard, off1) =>
            val off2 = shardToOffsets(shard)
            if (off1 != off2) {
              findUnEquals = true
            }
          }
          !findUnEquals
        }
      case _ => false
    }
  }
}
// scalastyle:on

case class DatahubShardOffset(project: String, topic: String, shard: String, offset: Long)
  extends PartitionOffset

case class DatahubShardOffsets(shardId: String, startOffset: String, endOffset: Long)
  extends PartitionOffset

case class DatahubOffset(sequenceId: Long, recordTime: Long)

object DatahubSourceOffset {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def getShardOffsets(offset: Offset): Map[DatahubShard, Long] = {
    offset match {
      case o: DatahubSourceOffset => o.shardToOffsets
      case so: SerializedOffset => DatahubSourceOffset(so).shardToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to DatahubSourceOffset")
    }
  }

  def partitionOffsets(shardToOffsets: Map[DatahubShard, Long]): String = {
    val result = new HashMap[String, HashMap[String, String]]()
    implicit val topicOrdering: Ordering[DatahubShard] =
      Ordering.by(t => (t.project, t.topic, t.shardId))
    val shards = shardToOffsets.keySet.toSeq.sorted  // sort for more determinism
    shards.foreach { shard =>
      val off = shardToOffsets(shard)
      val parts = result.getOrElse(DatahubInfo(shard.project, shard.topic).toString,
        new HashMap[String, String])
      parts += shard.shardId -> off.toString
      result += DatahubInfo(shard.project, shard.topic).toString -> parts
    }
    Serialization.write(result)
  }

  def partitionOffsets(str: String): Map[DatahubShard, Long] = {
    try {
      Serialization.read[Map[String, Map[String, String]]](str)
        .flatMap { case (info, shardOffset) =>
          shardOffset.map { case (shard, offset) =>
            val project = info.split("#")(0)
            val topic = info.split("#")(1)
            DatahubShard(project, topic, shard) -> offset.toLong
          }
        }
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"""Expected
             |{
             |  "project-A#topic-B":{
             |    "0":"1778693008",
             |    "1":"1778693008"
             |  },
             |  "project-C#topic-D":{
             |    "5":"1778693008"
             |  }
             |}, got $str""")
    }
  }

  def apply(offsetTuples: (String, String, String, Long)*): DatahubSourceOffset = {
    DatahubSourceOffset(offsetTuples.map { case (p, t, sh, os) =>
      (DatahubShard(p, t, sh), os)
    }.toMap)
  }

  def apply(offset: SerializedOffset): DatahubSourceOffset = {
    DatahubSourceOffset(partitionOffsets(offset.json))
  }
}
