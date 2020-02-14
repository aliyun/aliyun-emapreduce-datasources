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
package org.apache.spark.streaming.aliyun.logservice

import scala.collection.mutable.ArrayBuffer

import com.aliyun.openservices.log.common.Consts.CursorMode

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

class LoghubRDD(
    @transient sc: SparkContext,
    project: String,
    logStore: String,
    consumerGroup: String,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    duration: Long,
    zkParams: Map[String, String],
    shardOffsets: ArrayBuffer[ShardOffsetRange],
    checkpointDir: String,
    commitBeforeNext: Boolean = true) extends RDD[String](sc, Nil) with Logging {
  @transient var client: LoghubClientAgent = _
  @transient var zkHelper: ZkHelper = _
  private val enablePreciseCount: Boolean =
    sc.getConf.getBoolean("spark.streaming.loghub.count.precise.enable", true)

  private def initialize(): Unit = {
    client = LoghubRDD.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
    zkHelper = LoghubRDD.getZkHelper(zkParams, checkpointDir, project, logStore)
  }

  override def count(): Long = {
    if (enablePreciseCount) {
      super.count()
    } else {
      try {
        val numShards = shardOffsets.size
        shardOffsets.map(shard => {
          val from = client.GetCursorTime(project, logStore, shard.shardId, shard.beginCursor)
            .GetCursorTime()
          val endCursor =
            client.GetCursor(project, logStore, shard.shardId, CursorMode.END).GetCursor()
          val to = client.GetCursorTime(project, logStore, shard.shardId, endCursor)
            .GetCursorTime()
          val res = client.GetHistograms(project, logStore, from, to, "", "*")
          if (!res.IsCompleted()) {
            logWarning(s"Failed to get complete count for [$project]-[$logStore]-" +
              s"[${shard.shardId}] from ${shard.beginCursor} to ${endCursor}, " +
              s"use ${res.GetTotalCount()} instead. " +
              s"This warning does not introduce any job failure, but may affect some information " +
              s"about this batch.")
          }
          (res.GetTotalCount() * 1.0D) / numShards
        }).sum.toLong
      } catch {
        case e: Exception =>
          logWarning(s"Failed to get statistics of rows in [$project]-[$logStore], use 0L " +
            s"instead. This warning does not introduce any job failure, but may affect some " +
            s"information about this batch.", e)
          0L
      }
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    try {
      val loghubIterator = new LoghubIterator(zkHelper, client, project, logStore,
        consumerGroup, shardPartition.shardId, shardPartition.startCursor,
        shardPartition.count.toInt, context, commitBeforeNext, shardPartition.logGroupStep)
      new InterruptibleIterator[String](context, loghubIterator)
    } catch {
      case _: Exception =>
        Iterator.empty.asInstanceOf[Iterator[String]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val rate = sc.getConf.get("spark.streaming.loghub.maxRatePerShard", "10000").toInt
    val logGroupStep = sc.getConf.get("spark.loghub.batchGet.step", "100").toInt
    val count = rate * duration / 1000
    shardOffsets.zipWithIndex.map { case (p, idx) =>
      new ShardPartition(id, idx, p.shardId, count, project, logStore, consumerGroup,
        accessKeyId, accessKeySecret, endpoint, p.beginCursor, logGroupStep)
        .asInstanceOf[Partition]
    }.toArray
  }

  private class ShardPartition(
      rddId: Int,
      partitionId: Int,
      val shardId: Int,
      val count: Long,
      project: String,
      logStore: String,
      consumerGroup: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      val startCursor: String,
      val logGroupStep: Int = 100) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId
    override def equals(other: Any): Boolean = super.equals(other)
    override def index: Int = partitionId
  }
}

// scalastyle:off
object LoghubRDD extends Logging {
  private var zkHelper: ZkHelper = _
  private var loghubClient: LoghubClientAgent = _

  def getOrCreateLoghubClient(accessKeyId: String, accessKeySecret: String,
                              endpoint: String): LoghubClientAgent = {
    if (loghubClient == null) {
      loghubClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
    }
    loghubClient
  }

  def getZkHelper(zkParams: Map[String, String],
                  checkpointDir: String,
                  project: String,
                  logstore: String): ZkHelper = {
    if (zkHelper == null) {
      zkHelper = new ZkHelper(zkParams, checkpointDir, project, logstore)
      zkHelper.initialize()
    }
    zkHelper
  }

  override def finalize(): Unit = {
    super.finalize()
    try {
      if (zkHelper != null) {
        zkHelper.close()
        zkHelper = null
      }
    } catch {
      case e: Exception => logWarning("Exception when close zkClient.", e)
    }
  }
}
