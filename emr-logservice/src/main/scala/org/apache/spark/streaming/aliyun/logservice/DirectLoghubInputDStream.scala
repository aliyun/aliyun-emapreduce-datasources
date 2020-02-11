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

import java.{util => ju}
import java.io.{IOException, ObjectInputStream}
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue

// scalastyle:off
import scala.collection.JavaConversions._
// scalastyle:on
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.common.ConsumerGroup
import com.aliyun.openservices.log.exception.LogException
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.Utils

// scalastyle:off
class DirectLoghubInputDStream(
    _ssc: StreamingContext,
    project: String,
    logStore: String,
    consumerGroup: String,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    zkParams: Map[String, String],
    mode: LogHubCursorPosition,
    cursorStartTime: Long = -1L)
  extends InputDStream[String](_ssc) with Logging with CanCommitOffsets {
  @transient private var zkClient: ZkClient = null
  @transient private var loghubClient: LoghubClientAgent = null
  @transient private var COMMIT_LOCK = new Object()
  private val zkConnect = zkParams.getOrElse("zookeeper.connect", "localhost:2181")
  private val zkSessionTimeoutMs = zkParams.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
  private val zkConnectionTimeoutMs =
    zkParams.getOrElse("zookeeper.connection.timeout.ms", zkSessionTimeoutMs.toString).toInt
  private val enablePreciseCount: Boolean =
    _ssc.sparkContext.getConf.getBoolean("spark.streaming.loghub.count.precise.enable", true)
  private var checkpointDir: String = null
  private var readOnlyShardCache = new mutable.HashMap[Int, String]()
  private val commitQueue = new ConcurrentLinkedQueue[ShardOffsetRange]
  private val latestOffsets = new mutable.HashMap[Int, String]()

  override def start(): Unit = {
    if (enablePreciseCount) {
      logWarning(s"Enable precise count on loghub rdd, we will submit a count job in each batch. " +
        s"This value is showed in each batch job in streaming tab in Web UI. This may increase " +
        s"the total process time in each batch. You can disable the behavior by setting " +
        s"'spark.streaming.loghub.count.precise.enable'=false")
    } else {
      logWarning(s"Disable precise count on loghub rdd, we will get an approximate count of " +
        s"loghub rdd, via the `GetHistograms` api of log service. You can enable the behavior " +
        s"by setting 'spark.streaming.loghub.count.precise.enable'=true")
    }
    var zkCheckpointDir = ssc.checkpointDir
    if (StringUtils.isBlank(zkCheckpointDir)) {
      zkCheckpointDir = s"/$consumerGroup"
      logInfo(s"Checkpoint dir was not specified, using consumer group $consumerGroup as " +
        s"checkpoint dir")
    }
    checkpointDir = new Path(zkCheckpointDir).toUri.getPath
    createZkClient()

    try {
      // Check if zookeeper is usable. Direct loghub api depends on zookeeper.
      val exists = zkClient.exists(s"$checkpointDir")
      if (!exists) {
        zkClient.createPersistent(s"$checkpointDir/consume/$project/$logStore", true)
        zkClient.createPersistent(s"$checkpointDir/commit/$project/$logStore", true)
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Loghub direct api depends on zookeeper. Make sure that " +
          "zookeeper is on active service.", e)
    }
    loghubClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)

    tryToCreateConsumerGroup()

    // Clean commit data in zookeeper in case of restarting streaming job but lose checkpoint file
    // in `checkpointDir`
    try {
      zkClient.getChildren(s"$checkpointDir/commit/$project/$logStore").foreach(child => {
        zkClient.delete(s"$checkpointDir/commit/$project/$logStore/$child")
      })
    } catch {
      case _: ZkNoNodeException =>
        logDebug("If this is the first time to run, it is fine to not find any commit data in " +
          "zookeeper.")
    }
  }

  override def stop(): Unit = {
    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }
  }

  override def compute(validTime: Time): Option[RDD[String]] = {
    logInfo(s"compute validTime $validTime")
    val shardOffsets = new ArrayBuffer[ShardOffsetRange]()
    var checkpoints: mutable.Map[Int, String] = null

    loghubClient.ListShard(project, logStore).GetShards().foreach(shard => {
      val shardId = shard.GetShardId()
      val isReadonly = shard.getStatus.equalsIgnoreCase("readonly")
      if (isReadonly && readOnlyShardCache.contains(shardId)) {
        logInfo(s"There is no data to consume from shard $shardId.")
      } else {
        val start = try {
          zkClient.readData(s"$checkpointDir/consume/$project/$logStore/$shardId.shard")
        } catch {
          case _: ZkNoNodeException =>
            logWarning("Loghub checkpoint was lost in zookeeper, re-fetch from loghub")
            if (checkpoints == null) {
              checkpoints = fetchAllCheckpoints()
            }
            findCheckpointOrCursorForShard(shardId, checkpoints)
        }
        val end =
          loghubClient.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
        if (start.equals(end)) {
          logInfo(s"Skip shard $shardId which start and end cursor both are $start")
          if (isReadonly) {
            readOnlyShardCache.put(shardId, end)
          }
        } else {
          val currentCursor = latestOffsets.getOrElse(shardId, null)
          if (currentCursor != null && currentCursor.equals(start)) {
            logInfo(s"Skip shard $shardId as it's start is same as previous $start")
          } else {
            shardOffsets.add(ShardOffsetRange(shardId, start, end))
            logInfo(s"ShardID $shardId, start=$start end=$end")
          }
        }
      }
    })
    val rdd = new LoghubRDD(
      ssc.sc,
      project,
      logStore,
      accessKeyId,
      accessKeySecret,
      endpoint,
      ssc.graph.batchDuration.milliseconds,
      zkParams,
      shardOffsets,
      checkpointDir).setName(s"LoghubRDD-${validTime.toString()}")
    val description = shardOffsets.map { p =>
      val offset = "offset: [ %1$-30s to %2$-30s ]".format(p.beginCursor, p.endCursor)
      s"shardId: ${p.shardId}\t $offset"
    }.mkString("\n")
    val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
    if (storageLevel != StorageLevel.NONE) {
      // If storageLevel is not `StorageLevel.NONE`, we need to persist rdd before `count()` to
      // to count the number of records to avoid refetching data from loghub.
      rdd.persist(storageLevel)
      logDebug(s"Persisting RDD ${rdd.id} for time $validTime to $storageLevel")
    }
    val inputInfo = StreamInputInfo(id, rdd.count(), metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    shardOffsets.foreach(r => latestOffsets.put(r.shardId, r.beginCursor))
    commitAll()
    Some(rdd)
  }

  /**
   * Commit the offsets to LogService at a future time. Threadsafe.
   * Users should call this method at end of each outputOp, otherwise streaming offsets will never
   * go forward, i.e. re-compute the same data over and over again.
   */
  override def commitAsync(): Unit = {
    try {
      zkClient.getChildren(s"$checkpointDir/commit/$project/$logStore").foreach(child => {
        val shardId = child.substring(0, child.indexOf(".")).toInt
        if (!readOnlyShardCache.contains(shardId)) {
          val checkpoint: String = zkClient.readData(s"$checkpointDir/commit/$project/$logStore/$child")
          logInfo(s"Commit async shard $shardId, cursor $checkpoint")
          if (StringUtils.isNotBlank(checkpoint)) {
            commitQueue.add(ShardOffsetRange(shardId, checkpoint, null))
          }
        }
      })
    } catch {
      case _: ZkNoNodeException =>
        logWarning("If this is the first time to run, it is fine to not find any commit " +
          "data in zookeeper.")
    }
  }

  private def decodeCursor(cursor: String): Long = {
    val timestampAsBytes = ju.Base64.getDecoder.decode(cursor.getBytes(StandardCharsets.UTF_8))
    val timestamp = new String(timestampAsBytes, StandardCharsets.UTF_8)
    timestamp.toLong
  }

  private def maxCursor(lhs: String, rhs: String): String = {
    val lts = decodeCursor(lhs)
    val rts = decodeCursor(rhs)
    if (lts < rts) {
      rhs
    } else {
      lhs
    }
  }

  private def commitAll(): Unit = {
    val m = new ju.HashMap[Int, String]()
    var head = commitQueue.poll()
    while (null != head) {
      val shard = head.shardId
      val x = m.get(shard)
      val offset = if (null == x) {
        head.beginCursor
      } else {
        maxCursor(x, head.beginCursor)
      }
      m.put(shard, offset)
      head = commitQueue.poll()
    }
    if (!m.isEmpty) {
      m.foreach(offset => {
        val shard = offset._1
        val cursor = offset._2
        logInfo(s"Updating checkpoint $cursor of shard $shard to consumer " +
          s"group $consumerGroup")
        loghubClient.UpdateCheckPoint(project, logStore, consumerGroup, shard, cursor)
        DirectLoghubInputDStream.writeDataToZK(zkClient,
          s"$checkpointDir/consume/$project/$logStore/$shard.shard", cursor)
      })
    }
  }

  private[streaming] override def name: String = s"Loghub direct stream [$id]"

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    // TODO: move following logic to proper restart code path.
    this.synchronized {
      logDebug(s"${this.getClass.getSimpleName}.readObject used")
      ois.defaultReadObject()
      generatedRDDs = new mutable.HashMap[Time, RDD[String]]()
      readOnlyShardCache = new mutable.HashMap[Int, String]()
      COMMIT_LOCK = new Object()
      createZkClient()
    }
    loghubClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
  }

  private def createZkClient(): Unit = {
    zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
    zkClient.setZkSerializer(new ZkSerializer() {
      override def serialize(data: scala.Any): Array[Byte] = {
        if (data == null) {
          return null
        }
        data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        if (bytes == null) {
          return null
        }
        new String(bytes, StandardCharsets.UTF_8)
      }
    })
  }

  private def tryToCreateConsumerGroup(): Unit = {
    try {
      loghubClient
        .CreateConsumerGroup(project, logStore, new ConsumerGroup(consumerGroup, 10, true))
    } catch {
      case e: LogException =>
        if (e.GetErrorCode.compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0) {
          try {
            val consumerGroups =
              loghubClient.ListConsumerGroup(project, logStore).GetConsumerGroups()
            consumerGroups.count(cg => cg.getConsumerGroupName.equals(consumerGroup)) match {
              case 1 =>
                logInfo("Create consumer group successfully.")
              case 0 =>
                throw new LogHubClientWorkerException("consumer group not exist")
            }
          } catch {
            case e1: LogException =>
              throw new LogHubClientWorkerException("error occurs when get consumer group, " +
                "errorCode: " + e1.GetErrorCode + ", errorMessage: " + e1.GetErrorMessage)
          }
        } else {
          throw new LogHubClientWorkerException("error occurs when create consumer group, " +
            "errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage())
        }
    }
  }

  private def fetchAllCheckpoints(): mutable.Map[Int, String] = {
    val checkpoints = new mutable.HashMap[Int, String]()
    try {
      val result = loghubClient.ListCheckpoints(project, logStore, consumerGroup)
      if (result != null) {
        result.getCheckPoints.foreach(item => {
          val checkpoint = item.getCheckPoint
          if (StringUtils.isNotBlank(checkpoint)) {
            checkpoints.put(item.getShard, checkpoint)
          }
        })
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Cannot fetch checkpoint from remote server", e)
    }
    checkpoints
  }

  private def findCheckpointOrCursorForShard(shardId: Int, checkpoints: mutable.Map[Int, String]):
    String = {
    val checkpoint = checkpoints.getOrElse(shardId, null)
    if (StringUtils.isNotBlank(checkpoint)) {
      logInfo(s"Shard $shardId will start from checkpoint $checkpoint")
      return checkpoint
    }
    val cursor = mode match {
      case LogHubCursorPosition.END_CURSOR =>
        loghubClient.GetCursor(project, logStore, shardId, CursorMode.END)
      case LogHubCursorPosition.BEGIN_CURSOR =>
        loghubClient.GetCursor(project, logStore, shardId, CursorMode.BEGIN)
      case LogHubCursorPosition.SPECIAL_TIMER_CURSOR =>
        loghubClient.GetCursor(project, logStore, shardId, cursorStartTime)
    }
    cursor.GetCursor()
  }

  private class DirectLoghubInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    override def update(time: Time): Unit = {}

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {}
  }

  override def finalize(): Unit = {
    super.finalize()
    stop()
  }
}

// scalastyle:on

object DirectLoghubInputDStream {
  def writeDataToZK(zkClient: ZkClient, path: String, data: String): Unit = {
    val nodePath = new Path(path).toUri.getPath
    if (zkClient.exists(nodePath)) {
      zkClient.writeData(nodePath, data)
    } else {
      zkClient.createPersistent(nodePath, true)
      zkClient.writeData(nodePath, data)
    }
  }
}
