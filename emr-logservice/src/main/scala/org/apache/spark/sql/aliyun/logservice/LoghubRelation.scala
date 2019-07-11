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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType

class LoghubRelation(
    override val sqlContext: SQLContext,
    override val schema: StructType,
    sourceOptions: Map[String, String],
    startingOffsets: LoghubOffsetRangeLimit,
    endingOffsets: LoghubOffsetRangeLimit)
    extends BaseRelation with TableScan with InsertableRelation with Serializable with Logging {

  override def buildScan(): RDD[Row] = {
    val loghubOffsetReader = new LoghubOffsetReader(sourceOptions)
    val (fromPartitionOffsets, untilPartitionOffsets) = {
      try {
        (getPartitionOffsets(loghubOffsetReader, startingOffsets),
          getPartitionOffsets(loghubOffsetReader, endingOffsets))
      } finally {
        loghubOffsetReader.close()
      }
    }
    if (fromPartitionOffsets.keySet != untilPartitionOffsets.keySet) {
      implicit val topicOrdering: Ordering[LoghubShard] = Ordering.by(t => (t.logProject, t.logStore, t.shard))
      val fromTopics = fromPartitionOffsets.keySet.toList.sorted.mkString(",")
      val untilTopics = untilPartitionOffsets.keySet.toList.sorted.mkString(",")
      throw new IllegalStateException("different shards " +
        s"for starting offsets shards[$fromTopics] and " +
        s"ending offsets shards[$untilTopics]")
    }

    val logProject = sourceOptions("sls.project")
    val logStore = sourceOptions("sls.store")
    val accessKeyId = sourceOptions("access.key.id")
    val accessKeySecret = sourceOptions("access.key.secret")
    val endpoint = sourceOptions("endpoint")
    val shardOffsets = new ArrayBuffer[(Int, Int, Int)]()
    fromPartitionOffsets.foreach { case (loghubShard, sof) => {
      val eof = untilPartitionOffsets(loghubShard)
      shardOffsets.+=((loghubShard.shard, sof, eof))
    }}
    val rdd = new LoghubSourceRDD(sqlContext.sparkContext, logProject, logStore,
      accessKeyId, accessKeySecret, endpoint, shardOffsets, schema.fieldNames, sourceOptions)
      .mapPartitions(it => {
        val encoderForDataColumns = RowEncoder(schema).resolveAndBind()
        it.map(t => Utils.transFunc(t, encoderForDataColumns))
      })
    sqlContext.internalCreateDataFrame(rdd, schema).rdd
  }

  private def getPartitionOffsets(
      loghubReader: LoghubOffsetReader,
      loghubOffsets: LoghubOffsetRangeLimit): Map[LoghubShard, Int] = {
    def validateTopicPartitions(
        shards: Set[LoghubShard],
        shardOffsets: Map[LoghubShard, Int]): Map[LoghubShard, Int] = {
      assert(shards == shardOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all LogProject-LogStore-Shard.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${shardOffsets.keySet} Assigned: $shards")
      logDebug(s"Shards assigned to consumer: $shards. Seeking to $shardOffsets")
      shardOffsets
    }
    val shards = loghubReader.fetchLoghubShard()
    // Obtain TopicPartition offsets with late binding support
    loghubOffsets match {
      case EarliestOffsetRangeLimit =>
        val earliest = loghubReader.fetchEarliestOffsets()
        shards.map {
          case tp => tp -> earliest(tp)
        }.toMap
      case LatestOffsetRangeLimit =>
        val latest = loghubReader.fetchLatestOffsets()
        shards.map {
          case tp => tp -> latest(tp)
        }.toMap
      case SpecificOffsetRangeLimit(shardOffsets) =>
        validateTopicPartitions(shards, shardOffsets)
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    LoghubWriter.write(sqlContext.sparkSession, data.queryExecution, sourceOptions)
  }

  override def toString: String =
    s"LoghubRelation(start=$startingOffsets, end=$endingOffsets)"
}
