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

package org.apache.spark.streaming.aliyun.datahub

import java.io.UnsupportedEncodingException
import java.nio.charset.StandardCharsets

import com.aliyun.datahub.DatahubConfiguration
import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.model.RecordEntry
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.collection.mutable.ArrayBuffer

class DatahubRDD(
    @transient _sc: SparkContext,
    duration: Long,
    endpoint: String,
    project: String,
    topic: String,
    subId: String,
    accessId: String,
    accessKey: String,
    func: RecordEntry => String,
    shardOffsets: ArrayBuffer[(String, String, String)],
    zkParam: Map[String, String],
    checkpointDir: String) extends RDD[Array[Byte]](_sc, Nil) with Logging{

  @transient private var zkClient = DatahubRDD.getClient(zkParam, accessId, accessKey, endpoint)._1
  @transient private var datahubClientAgent = DatahubRDD.getClient(zkParam, accessId, accessKey, endpoint)._2

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    zkClient = DatahubRDD.getClient(zkParam, accessId, accessKey, endpoint)._1
    zkClient.setZkSerializer(new ZkSerializer{
      override def serialize(data: scala.Any): Array[Byte] = {
        data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        new String(bytes, StandardCharsets.UTF_8)
      }
    })
    datahubClientAgent = DatahubRDD.getClient(zkParam, accessId, accessKey, endpoint)._2

    val partition = split.asInstanceOf[ShardPartition]
    try {
      new InterruptibleIterator[Array[Byte]](context, new DatahubIterator(datahubClientAgent, endpoint, project, topic, partition.shardId, partition.cursor,
        partition.count, subId, accessId, accessKey, func, zkClient, checkpointDir, context))
    } catch {
      case e: Exception =>
        logError("Fail to build DatahubIterator.", e)
        Iterator.empty.asInstanceOf[Iterator[Array[Byte]]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val rate = _sc.getConf.get("spark.streaming.datahub.maxRatePerShard", "10000").toInt
    var count = rate * duration / 1000

    datahubClientAgent = DatahubRDD.getClient(zkParam, accessId, accessKey, endpoint)._2
    shardOffsets.zipWithIndex.map {
      case (so, index) =>
        val shardId = so._1
        val startOffset = JacksonParser.getOffset(so._2)
        val endOffset = JacksonParser.getOffset(so._3)
        if (count > endOffset.getSequence - startOffset.getSequence + 1) {
          count = endOffset.getSequence - startOffset.getSequence + 1
        }
        val cursor = datahubClientAgent.getCursor(project, topic, shardId, startOffset).getCursor
        new ShardPartition(id, shardId, index, cursor, count.toInt).asInstanceOf[Partition]
    }.toArray
  }
}

private class ShardPartition(
    rddId: Int,
    val shardId: String,
    partitionId: Int,
    val cursor: String,
    val count: Int) extends Partition {
  override def index: Int = partitionId
  override def hashCode(): Int = 41 * (41 + rddId) + index
}

object DatahubRDD extends Logging {
  var zkClient: ZkClient = null
  var datahubClient: DatahubClientAgent = null

  def getClient(zkParam: Map[String, String], accessId: String, accessKey: String, endpoint: String): (ZkClient, DatahubClientAgent) = {
    if (zkClient ==  null || datahubClient == null) {
      val zkServers = zkParam.getOrElse("zookeeper.connect", "localhost:2181")
      val sessionTimeout = zkParam.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
      val connectionTimeout = zkParam.getOrElse("zookeeper.connection.timeout.ms", sessionTimeout.toString).toInt
      zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout)
      zkClient.setZkSerializer(new ZkSerializer {
        override def serialize(data: scala.Any): Array[Byte] = {
          try {
            data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
          } catch {
            case e: UnsupportedEncodingException =>
              logError("Fail to store data to Zookeeper.", e)
              null
          }
        }

        override def deserialize(bytes: Array[Byte]): AnyRef = {
          if (bytes == null) {
            return null
          }
          try {
            new String(bytes, StandardCharsets.UTF_8)
          } catch{
            case e: UnsupportedEncodingException =>
              logError("Fail to get data from Zookeeper.", e)
              null
          }
        }
      })
      datahubClient = new DatahubClientAgent(new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint))
    }

    (zkClient, datahubClient)
  }

  override def finalize(): Unit = {
    super.finalize()
    if (zkClient != null) {
      try {
        zkClient.close()
        zkClient = null
      } catch {
        case e:Exception =>
          logError("Catch exception when close Zookeeper client", e)
      }
    }
  }
}