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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.{PartitionOffset, Offset => OffsetV2}

case class LoghubSourceOffset(shardToOffsets: Map[LoghubShard, Int]) extends OffsetV2 {
  override def json(): String = LoghubSourceOffset.partitionOffsets(shardToOffsets)
}

case class LoghubShardOffset(logProject: String, logStore: String, shard: Int, offset: Int) extends PartitionOffset

object LoghubSourceOffset {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def getShardOffsets(offset: Offset): Map[LoghubShard, Int] = {
    offset match {
      case o: LoghubSourceOffset => o.shardToOffsets
      case so: SerializedOffset => LoghubSourceOffset(so).shardToOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to LoghubSourceOffset")
    }
  }

  def partitionOffsets(shardToOffsets: Map[LoghubShard, Int]): String = {
    val result = new HashMap[String, HashMap[Int, Int]]()
    implicit val topicOrdering: Ordering[LoghubShard] = Ordering.by(t => (t.logProject, t.logStore, t.shard))
    val shards = shardToOffsets.keySet.toSeq.sorted  // sort for more determinism
    shards.foreach { shard =>
      val off = shardToOffsets(shard)
      val parts = result.getOrElse(Loghub(shard.logProject, shard.logStore).toString, new HashMap[Int, Int])
      parts += shard.shard -> off
      result += Loghub(shard.logProject, shard.logStore).toString -> parts
    }
    Serialization.write(result)
  }

  def partitionOffsets(str: String): Map[LoghubShard, Int] = {
    try {
      Serialization.read[Map[String, Map[Int, Int]]](str).flatMap { case (log, shardOffset) =>
        shardOffset.map { case (shard, offset) =>
          val logProject = log.split("#")(0)
          val logStore = log.split("#")(1)
          LoghubShard(logProject, logStore, shard) -> offset
        }
      }
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"""Expected
             |{
             |  "logProject-A#logStore-B":{
             |    "0":"1409569200",
             |    "1":"1409569200"
             |  },
             |  "logProject-C#logStore-D":{
             |    "5":"1409569200"
             |  }
             |}, got $str""")
    }
  }

  def apply(offsetTuples: (String, String, Int, Int)*): LoghubSourceOffset = {
    LoghubSourceOffset(offsetTuples.map { case (p, ls, sh, os) => (LoghubShard(p, ls, sh), os)}.toMap )
  }

  def apply(offset: SerializedOffset): LoghubSourceOffset = {
    LoghubSourceOffset(partitionOffsets(offset.json))
  }
}