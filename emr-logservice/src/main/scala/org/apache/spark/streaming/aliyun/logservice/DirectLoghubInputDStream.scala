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

import java.io.{IOException, ObjectInputStream}
import java.nio.charset.StandardCharsets

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
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DirectLoghubInputDStream(_ssc: StreamingContext,
                               project: String,
                               logStore: String,
                               consumerGroup: String,
                               accessKeyId: String,
                               accessKeySecret: String,
                               endpoint: String,
                               zkParams: Map[String, String],
                               mode: LogHubCursorPosition,
                               cursorStartTime: Long = -1L
                              ) extends InputDStream[String](_ssc) with Logging with CanCommitOffsets {
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
  private var doCommit: Boolean = false
  @transient private var startTime: Long = -1L
  @transient private var restart: Boolean = false
  private var lastJobTime: Long = -1L
  private var readOnlyShardCache = new mutable.HashMap[Int, String]()

  override def start(): Unit = {
    if (enablePreciseCount) {
      logWarning(s"Enable precise count on loghub rdd, we will submit a count job in each batch. " +
        s"This value is showed in each batch job in streaming tab in Web UI. This may increase the " +
        s"total process time in each batch. You can disable the behavior by setting " +
        s"'spark.streaming.loghub.count.precise.enable'=false")
    } else {
      logWarning(s"Disable precise count on loghub rdd, we will get an approximate count of loghub rdd, " +
        s"via the `GetHistograms` api of log service. You can enable the behavior by setting " +
        s"'spark.streaming.loghub.count.precise.enable'=true")
    }
    // TODO: support concurrent jobs
    val concurrentJobs = ssc.conf.getInt(s"spark.streaming.concurrentJobs", 1)
    require(concurrentJobs == 1, "Loghub direct api only supports one job concurrently, " +
      "but \"spark.streaming.concurrentJobs\"=" + concurrentJobs)

    var zkCheckpointDir = ssc.checkpointDir
    if (StringUtils.isBlank(zkCheckpointDir)) {
      zkCheckpointDir = s"/$consumerGroup"
      logInfo(s"Checkpoint dir was not specified, using consumer group $consumerGroup as checkpoint dir")
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

    import scala.collection.JavaConversions._
    val initial = if (zkClient.exists(s"$checkpointDir/consume/$project/$logStore")) {
      zkClient.getChildren(s"$checkpointDir/consume/$project/$logStore")
        .filter(_.endsWith(".shard")).map(_.stripSuffix(".shard").toInt).toArray
    } else {
      Array.empty[String]
    }
    val diff = loghubClient.ListShard(project, logStore).GetShards().map(_.GetShardId()).diff(initial)
    if (diff.nonEmpty) {
      val checkpoints = fetchAllCheckpoints()
      diff.foreach(shardId => {
        val cursor = findCheckpointOrCursorForShard(shardId, checkpoints)
        DirectLoghubInputDStream.writeDataToZK(zkClient, s"$checkpointDir/consume/$project/$logStore/$shardId.shard", cursor)
      })
    }

    // Clean commit data in zookeeper in case of restarting streaming job but lose checkpoint file
    // in `checkpointDir`
    import scala.collection.JavaConversions._
    try {
      zkClient.getChildren(s"$checkpointDir/commit/$project/$logStore").foreach(child => {
        zkClient.delete(s"$checkpointDir/commit/$project/$logStore/$child")
      })
      doCommit = false
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
    if (startTime == -1L) {
      startTime = if (restart) {
        val originalStartTime = graph.zeroTime.milliseconds
        val period = graph.batchDuration.milliseconds
        val gap = System.currentTimeMillis() - originalStartTime
        (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
      } else {
        validTime.milliseconds
      }
    }

    COMMIT_LOCK.synchronized {
      val shardOffsets = new ArrayBuffer[(Int, String, String)]()
      val lastFailed: Boolean = {
        if (doCommit) {
          false
        } else {
          val pendingTimes = ssc.scheduler.getPendingTimes()
          val pending = pendingTimes.exists(time => time.milliseconds == lastJobTime)
          if (pending) {
            false
          } else {
            true
          }
        }
      }
      val rdd = if (validTime.milliseconds > startTime && (doCommit || lastFailed)) {
        if (restart || lastFailed) {
          // 1. At the first time after restart, we should recompute from the last `consume`
          // offset. (TODO: confirm this logic?)
          // 2. If last batch job failed, we should also recompute from last `consume` offset.
          // Then, set `restart=false` to continue committing.
          restart = false
        } else {
          commitAll()
        }
        try {
          loghubClient.ListShard(project, logStore).GetShards().foreach(shard => {
            val shardId = shard.GetShardId()
            val isReadonly = shard.getStatus.equalsIgnoreCase("readonly")
            if (isReadonly && readOnlyShardCache.contains(shardId)) {
              logDebug(s"There is no data to consume from shard $shardId.")
            } else {
              val start: String = zkClient.readData(s"$checkpointDir/consume/$project/$logStore/$shardId.shard")
              val end = loghubClient.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
              logInfo(s"ShardID $shardId, start $start end $end")
              if (!start.equals(end)) {
                shardOffsets.+=((shardId, start, end))
              }
              if (start.equals(end) && isReadonly) {
                readOnlyShardCache.put(shardId, end)
              }
            }
          })
        } catch {
          case _: ZkNoNodeException =>
            logWarning("Loghub consuming metadata was lost in zookeeper, re-fetch from loghub " +
              "checkpoint")
            val checkpoints = fetchAllCheckpoints()
            loghubClient.ListShard(project, logStore).GetShards().foreach(shard => {
              val shardId = shard.GetShardId()
              val start: String = findCheckpointOrCursorForShard(shardId, checkpoints)
              val end = loghubClient.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
              logInfo(s"ShardID $shardId, start $start end $end")
              shardOffsets.+=((shardId, start, end))
            })
        }
        lastJobTime = validTime.milliseconds
        new LoghubRDD(
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
      } else {
        // Last batch has not been completed, here we generator a fake job containing no data to
        // skip this batch.
        new FakeLoghubRDD(ssc.sc).setName(s"Empty-LoghubRDD-${validTime.toString()}")
      }

      val description = shardOffsets.map { p =>
        val offset = "offset: [ %1$-30s to %2$-30s ]".format(p._2, p._3)
        s"shardId: ${p._1}\t $offset"
      }.mkString("\n")
      val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
      if (storageLevel != StorageLevel.NONE && rdd.isInstanceOf[LoghubRDD]) {
        // If storageLevel is not `StorageLevel.NONE`, we need to persist rdd before `count()` to
        // to count the number of records to avoid refetching data from loghub.
        rdd.persist(storageLevel)
        logDebug(s"Persisting RDD ${rdd.id} for time $validTime to $storageLevel")
      }
      val inputInfo = StreamInputInfo(id, rdd.count, metadata)
      ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
      Some(rdd)
    }
  }

  /**
   * Commit the offsets to LogService at a future time. Threadsafe.
   * Users should call this method at end of each outputOp, otherwise streaming offsets will never
   * go forward, i.e. re-compute the same data over and over again.
   */
  override def commitAsync(): Unit = {
    COMMIT_LOCK.synchronized {
      doCommit = true
    }
  }

  private def commitAll(): Unit = {
    if (doCommit) {
      import scala.collection.JavaConversions._
      try {
        zkClient.getChildren(s"$checkpointDir/commit/$project/$logStore").foreach(child => {
          val shardId = child.substring(0, child.indexOf(".")).toInt
          if (!readOnlyShardCache.contains(shardId)) {
            val data: String = zkClient.readData(s"$checkpointDir/commit/$project/$logStore/$child")
            log.debug(s"Updating checkpoint $data of shard $shardId to consumer group $consumerGroup")
            loghubClient.UpdateCheckPoint(project, logStore, consumerGroup, shardId, data)
            DirectLoghubInputDStream.writeDataToZK(zkClient, s"$checkpointDir/consume/$project/$logStore/$child", data)
          }
        })
        doCommit = false
      } catch {
        case _: ZkNoNodeException =>
          logWarning("If this is the first time to run, it is fine to not find any commit data in " +
            "zookeeper.")
          doCommit = false
      }
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
    startTime = -1L
    restart = true
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
      loghubClient.CreateConsumerGroup(project, logStore, new ConsumerGroup(consumerGroup, 10, true))
    } catch {
      case e: LogException =>
        if (e.GetErrorCode.compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0) {
          try {
            val consumerGroups = loghubClient.ListConsumerGroup(project, logStore).GetConsumerGroups()
            import scala.collection.JavaConversions._
            consumerGroups.count(cg => cg.getConsumerGroupName.equals(consumerGroup)) match {
              case 1 =>
                logInfo("Create consumer group successfully.")
              case 0 =>
                throw new LogHubClientWorkerException("consumer group not exist")
            }
          } catch {
            case e1: LogException =>
              throw new LogHubClientWorkerException("error occurs when get consumer group, errorCode: " +
                e1.GetErrorCode + ", errorMessage: " + e1.GetErrorMessage)
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
          checkpoints.put(item.getShard, item.getCheckPoint)
        })
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Cannot fetch checkpoint from remote server", e)
    }
    checkpoints
  }

  private def findCheckpointOrCursorForShard(shardId: Int, checkpoints: mutable.Map[Int, String]): String = {
    val checkpoint = checkpoints.getOrElse(shardId, null)
    if (checkpoint != null) {
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
