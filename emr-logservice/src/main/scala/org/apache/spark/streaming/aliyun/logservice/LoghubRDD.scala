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

import java.io.UnsupportedEncodingException

import com.aliyun.openservices.log.common.LogGroupData
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

class LoghubRDD(
    @transient sc: SparkContext,
    project: String,
    logStore: String,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    duration: Long,
    zkParams: Map[String, String],
    shardOffsets: ArrayBuffer[(Int, String, String)],
    checkpointDir: String,
    logGroupDecoder: LogGroupData => ArrayBuffer[String])
  extends RDD[String](sc, Nil) with Logging {
  @transient var mClient: LoghubClientAgent =
    LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._2
  @transient var zkClient: ZkClient =
    LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._1
  private val enablePreciseCount: Boolean =
    sc.getConf.getBoolean("spark.streaming.loghub.count.precise.enable", true)
  private val decoder = logGroupDecoder

  private def initialize(): Unit = {
    mClient = LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._2
    zkClient = LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._1
  }

  override def count(): Long = {
    if (enablePreciseCount) {
      super.count()
    } else {
      try {
        val numShards = shardOffsets.size
        shardOffsets.map(shard => {
          val from = mClient.GetCursorTime(project, logStore, shard._1, shard._2).GetCursorTime()
          val to = mClient.GetCursorTime(project, logStore, shard._1, shard._3).GetCursorTime()
          val res = mClient.GetHistograms(project, logStore, from, to, "", "*")
          if (!res.IsCompleted()) {
            logWarning(s"Failed to get complete count for [$project]-[$logStore]-[${shard._1}] " +
              s"from ${shard._2} to ${shard._3}, use ${res.GetTotalCount()} instead. This warning " +
              s"does not introduce any job failure, but may affect some information about this batch.")
          }
          (res.GetTotalCount() * 1.0D) / numShards
        }).sum.toLong
      } catch {
        case e: Exception =>
          logWarning(s"Failed to get statistics of rows in [$project]-[$logStore], use 0L instead. " +
            s"This warning does not introduce any job failure, but may affect some information about " +
            s"this batch.", e)
          0L
      }
    }
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    try {
      val loghubIterator = new LoghubIterator(
        zkClient,
        mClient,
        project,
        logStore,
        shardPartition.shardId,
        shardPartition.startCursor,
        shardPartition.endCursor,
        shardPartition.count.toInt,
        checkpointDir,
        context,
        shardPartition.logGroupStep,
        decoder)
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
      new ShardPartition(id, idx, p._1, count, project, logStore,
        accessKeyId, accessKeySecret, endpoint, p._2, p._3, logGroupStep).asInstanceOf[Partition]
    }.toArray
  }

  private class ShardPartition(
      rddId: Int,
      partitionId: Int,
      val shardId: Int,
      val count: Long,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      val startCursor: String,
      val endCursor: String,
      val logGroupStep: Int = 100) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId
    override def index: Int = partitionId
  }
}

object LoghubRDD extends Logging {
  private var zkClient: ZkClient = null
  private var mClient: LoghubClientAgent = null

  def getClient(zkParams: Map[String, String], accessKeyId: String, accessKeySecret: String,
      endpoint: String): (ZkClient, LoghubClientAgent) = {
    if (zkClient == null || mClient == null) {
      val zkConnect = zkParams.getOrElse("zookeeper.connect", "localhost：2181")
      val zkSessionTimeoutMs = zkParams.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
      val zkConnectionTimeoutMs =
        zkParams.getOrElse("zookeeper.connection.timeout.ms", zkSessionTimeoutMs.toString).toInt

      zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
      zkClient.setZkSerializer(new ZkSerializer() {
        override def serialize(data: scala.Any): Array[Byte] = {
          try {
            data.asInstanceOf[String].getBytes("UTF-8")
          } catch {
            case e: UnsupportedEncodingException =>
              null
          }
        }

        override def deserialize(bytes: Array[Byte]): AnyRef = {
          if (bytes == null) {
            return null
          }
          try {
            new String(bytes, "UTF-8")
          } catch {
            case e: UnsupportedEncodingException =>
              null
          }
        }
      })

      mClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
    }

    (zkClient, mClient)
  }

  override def finalize(): Unit = {
    super.finalize()
    try {
      if (zkClient != null) {
        zkClient.close()
        zkClient = null
      }
    } catch {
      case e: Exception => logWarning("Exception when close zkClient.", e)
    }
  }
}
