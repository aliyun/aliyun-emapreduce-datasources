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
    checkpointDir: String) extends RDD[String](sc, Nil) with Logging {
  @transient var mClient: LoghubClientAgent =
    LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._2
  @transient var zkClient: ZkClient =
    LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._1

  private def initialize(): Unit = {
    mClient = LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._2
    zkClient = LoghubRDD.getClient(zkParams, accessKeyId, accessKeySecret, endpoint)._1
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    initialize()
    val shardPartition = split.asInstanceOf[ShardPartition]
    try {
      val loghubIterator = new LoghubIterator(zkClient, mClient, project, logStore,
        shardPartition.shardId, shardPartition.startCursor, shardPartition.endCursor,
        shardPartition.count.toInt, checkpointDir, context)
      new InterruptibleIterator[String](context, loghubIterator)
    } catch {
      case e: Exception =>
        Iterator.empty.asInstanceOf[Iterator[String]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val rate = sc.getConf.get("spark.streaming.loghub.maxRatePerShard", "10000").toInt
    val count = rate * duration / 1000
    shardOffsets.map(p =>
      new ShardPartition(id, p._1, count, project, logStore,
        accessKeyId, accessKeySecret, endpoint, p._2, p._3).asInstanceOf[Partition]
    ).toArray.sortWith(_.index < _.index)
  }

  private class ShardPartition(
      rddId: Int,
      val shardId: Int,
      val count: Long,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      val startCursor: String,
      val endCursor: String) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + shardId
    override def index: Int = shardId
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
