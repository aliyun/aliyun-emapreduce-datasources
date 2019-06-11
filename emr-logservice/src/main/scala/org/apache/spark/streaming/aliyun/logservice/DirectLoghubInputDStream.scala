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

import java.io.{IOException, ObjectInputStream, UnsupportedEncodingException}
import java.util
import java.util.Properties

import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.common.{ConsumerGroup, ConsumerGroupShardCheckPoint}
import com.aliyun.openservices.log.exception.LogException
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.Utils

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConversions._
import scala.collection.mutable

class DirectLoghubInputDStream(
    _ssc: StreamingContext,
    project: String,
    logStore: String,
    mConsumerGroup: String,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    zkParams: Map[String, String],
    mode: LogHubCursorPosition,
    cursorStartTime: Long = -1L
  ) extends InputDStream[String](_ssc) with Logging with CanCommitOffsets {
  @transient private var zkClient: ZkClient = null
  @transient private var mClient: LoghubClientAgent = null
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
      logWarning(s"Disable precise count on loghub rdd, we will get an approximative count of loghub rdd, " +
        s"via the `GetHistograms` api of log service. You can enable the behavior by setting " +
        s"'spark.streaming.loghub.count.precise.enable'=true")
    }

    val props = new Properties()
    zkParams.foreach(param => props.put(param._1, param._2))
    val autoCommit = zkParams.getOrElse("enable.auto.commit", "false").toBoolean
    // TODO: support concurrent jobs
    val concurrentJobs = ssc.conf.getInt(s"spark.streaming.concurrentJobs", 1)
    require(concurrentJobs == 1, "Loghub direct api only supports one job concurrently, " +
      "but \"spark.streaming.concurrentJobs\"=" + concurrentJobs)
    require(StringUtils.isNotEmpty(ssc.checkpointDir) && !autoCommit, "Disable auto commit by " +
      "setting \"enable.auto.commit=false\" and enable checkpoint.")
    checkpointDir = new Path(ssc.checkpointDir).toUri.getPath
    if (!autoCommit) {
      zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
      zkClient.setZkSerializer(new ZkSerializer() {
        override def serialize(data: scala.Any): Array[Byte] = {
          try {
            data.asInstanceOf[String].getBytes("UTF-8")
          } catch {
            case _: UnsupportedEncodingException =>
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
            case _: UnsupportedEncodingException =>
              null
          }
        }
      })
    }

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

    mClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)

    tryToCreateConsumerGroup()

    import scala.collection.JavaConversions._
    val initial = if (zkClient.exists(s"$checkpointDir/consume/$project/$logStore")) {
      zkClient.getChildren(s"$checkpointDir/consume/$project/$logStore")
        .filter(_.endsWith(".shard")).map(_.stripSuffix(".shard").toInt).toArray
    } else {
      Array.empty[String]
    }
    val diff = mClient.ListShard(project, logStore).GetShards().map(_.GetShardId()).diff(initial)
    diff.foreach(shardId => {
      val nextCursor = fetchCursorFromLoghub(shardId)
      DirectLoghubInputDStream.writeDataToZK(zkClient, s"$checkpointDir/consume/$project/$logStore/$shardId.shard",
        nextCursor)
    })

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
        // Do nothing, make compiler happy.
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
        if(doCommit) {
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
          mClient.ListShard(project, logStore).GetShards().foreach(shard => {
            val shardId = shard.GetShardId()
            if (shard.getStatus.toLowerCase.equals("readonly") && readOnlyShardCache.contains(shardId)) {
              // do nothing
              logDebug(s"There is no data to consume from shard $shardId.")
            } else {
              val start: String = zkClient.readData(s"$checkpointDir/consume/$project/$logStore/$shardId.shard")
              val end = mClient.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
              if (!start.equals(end)) {
                shardOffsets.+=((shardId, start, end))
              }
              if (start.equals(end) && shard.getStatus.toLowerCase.equals("readonly")) {
                readOnlyShardCache.put(shardId, end)
              }
            }
          })
        } catch {
          case _: ZkNoNodeException =>
            logWarning("Loghub consuming metadata was lost in zookeeper, re-fetch from loghub " +
              "checkpoint")
            mClient.ListShard(project, logStore).GetShards().foreach(shard => {
              val shardId = shard.GetShardId()
              val start: String = fetchCursorFromLoghub(shardId)
              val end = mClient.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
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
            log.info(s"Updating checkpoint $data of shard $shardId to consumer group $mConsumerGroup")
            mClient.UpdateCheckPoint(project, logStore, mConsumerGroup, shardId, data)
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
      generatedRDDs = new HashMap[Time, RDD[String]]()
      readOnlyShardCache = new mutable.HashMap[Int, String]()
      COMMIT_LOCK = new Object()
      val autoCommit = zkParams.getOrElse("enable.auto.commit", "true").toBoolean
      require(checkpointDir.nonEmpty || autoCommit, "Enable auto commit by setting " +
        "\"enable.auto.commit=true\" or enable checkpoint.")
      if (!autoCommit) {
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
              case _: UnsupportedEncodingException =>
                null
            }
          }
        })
      }
      mClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
      startTime = -1L
      restart = true
    }
  }

  private def tryToCreateConsumerGroup(): Unit = {
    try {
      mClient.CreateConsumerGroup(project, logStore, new ConsumerGroup(mConsumerGroup, 10, true))
    } catch {
      case e: LogException =>
        if (e.GetErrorCode.compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0) {
          try {
            val consumerGroups = mClient.ListConsumerGroup(project, logStore).GetConsumerGroups()
            import scala.collection.JavaConversions._
            consumerGroups.count(cg => cg.getConsumerGroupName.equals(mConsumerGroup)) match {
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

  private def fetchCursorFromLoghub(shardId: Int): String = {
    var checkPoints: util.ArrayList[ConsumerGroupShardCheckPoint] = null
    try {
      checkPoints = mClient.GetCheckPoint(project, logStore, mConsumerGroup, shardId)
        .GetCheckPoints()
    } finally {
      // Do nothing.
    }
    val checkpoint = if (CollectionUtils.isEmpty(checkPoints)) {
      logWarning(s"Can not find any checkpoint for specific consumer group $mConsumerGroup")
      null
    } else {
      checkPoints.get(0).getCheckPoint
    }

    val nextCursor = if (StringUtils.isNoneEmpty(checkpoint)) {
      checkpoint
    } else {
      val cursor = mode match {
        case LogHubCursorPosition.END_CURSOR =>
          mClient.GetCursor(project, logStore, shardId, CursorMode.END)
        case LogHubCursorPosition.BEGIN_CURSOR =>
          mClient.GetCursor(project, logStore, shardId, CursorMode.BEGIN)
        case LogHubCursorPosition.SPECIAL_TIMER_CURSOR =>
          mClient.GetCursor(project, logStore, shardId, cursorStartTime)
      }
      cursor.GetCursor()
    }

    nextCursor
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
