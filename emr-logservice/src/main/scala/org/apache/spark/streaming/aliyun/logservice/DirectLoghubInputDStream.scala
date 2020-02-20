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

// scalastyle:on
import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.common.ConsumerGroup
import com.aliyun.openservices.log.exception.LogException
import com.aliyun.openservices.log.response.ListShardResponse
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo

// scalastyle:off
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
  @transient private var zkHelper: ZkHelper = _
  @transient private var loghubClient: LoghubClientAgent = null

  private val enablePreciseCount: Boolean =
    _ssc.sparkContext.getConf.getBoolean("spark.streaming.loghub.count.precise.enable", true)
  private var checkpointDir: String = null
  private val readOnlyShardCache = new mutable.HashMap[Int, String]()
  private val readOnlyShardEndCursorCache = new mutable.HashMap[Int, String]()
  private var savedCheckpoints: mutable.Map[Int, String] = _
  private val commitInNextBatch = new ju.concurrent.atomic.AtomicBoolean(false)

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
    initialize()
  }

  private def initialize(): Unit = this.synchronized {
    if (loghubClient == null) {
      loghubClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
      tryToCreateConsumerGroup()
    }
    if (zkHelper == null) {
      zkHelper = new ZkHelper(zkParams, checkpointDir, project, logStore)
      zkHelper.initialize()
      zkHelper.mkdir()
      logInfo("Initializing zk client")
    }
  }

  def setClient(client: LoghubClientAgent): Unit = {
    loghubClient = client
  }

  def getSavedCheckpoints: mutable.Map[Int, String] = {
    savedCheckpoints
  }

  override def stop(): Unit = this.synchronized {
    if (zkHelper != null) {
      zkHelper.close()
      zkHelper = null
    }
  }

  private def restoreOrFetchInitialCursor(shardId: Int): String = {
    try {
      val cursor = zkHelper.readOffset(shardId)
      if (StringUtils.isNotBlank(cursor)) {
        return cursor
      }
      logWarning(s"Invalid offset [$cursor] found for shard $shardId")
    } catch {
      case _: ZkNoNodeException =>
        logWarning(s"No checkpoint restored from zk for shard $shardId")
    }
    findCheckpointOrCursorForShard(shardId, savedCheckpoints)
  }

  private def getShardCursorRange(shardId: Int, isReadonly: Boolean): (String, String) = {
    val start = restoreOrFetchInitialCursor(shardId)
    if (isReadonly) {
      var endCursor = readOnlyShardEndCursorCache.getOrElse(shardId, null)
      if (endCursor == null) {
        endCursor =
          loghubClient.GetCursor(project, logStore, shardId, CursorMode.END).GetCursor()
        readOnlyShardEndCursorCache.put(shardId, endCursor)
      }
      (start, endCursor)
    } else {
      // Do not fetch end cursor for performance concern.
      (start, null)
    }
  }

  override def compute(validTime: Time): Option[RDD[String]] = {
    initialize()
    val shardOffsets = new ArrayBuffer[ShardOffsetRange]()
    val commitFirst = commitInNextBatch.get()
    val resp: ListShardResponse = loghubClient.ListShard(project, logStore)
    resp.GetShards().foreach(shard => {
      val shardId = shard.GetShardId()
      if (readOnlyShardCache.contains(shardId)) {
        logInfo(s"There is no data to consume from shard $shardId.")
      } else if (zkHelper.tryLock(shardId)) {
        val isReadonly = shard.getStatus.equalsIgnoreCase("readonly")
        val r = getShardCursorRange(shardId, isReadonly)
        val start = r._1
        val end = r._2
        if (isReadonly && start.equals(end)) {
          logInfo(s"Skip shard $shardId which start and end cursor both are $start")
          // No more data in this shard. Commit it's offset for checkpointing.
          if (commitFirst &&
            loghubClient.safeUpdateCheckpoint(project, logStore, consumerGroup, shardId, start)) {
            readOnlyShardCache.put(shardId, end)
          }
          zkHelper.unlock(shardId)
        } else {
          shardOffsets.add(ShardOffsetRange(shardId, start, end))
          logInfo(s"Shard $shardId range [$start, $end)")
        }
      }
    })
    val rdd = new LoghubRDD(
      ssc.sc,
      project,
      logStore,
      consumerGroup,
      accessKeyId,
      accessKeySecret,
      endpoint,
      ssc.graph.batchDuration.milliseconds,
      zkParams,
      shardOffsets,
      checkpointDir,
      commitFirst).setName(s"LoghubRDD-${validTime.toString()}")
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
    if (commitFirst) {
      shardOffsets.foreach(r => savedCheckpoints.put(r.shardId, r.beginCursor))
      commitInNextBatch.set(false)
    }
    Some(rdd)
  }

  /**
   * Commit the offsets to LogService at a future time. Threadsafe.
   * Users should call this method at end of each outputOp.
   */
  override def commitAsync(): Unit = {
    commitInNextBatch.set(true)
  }

  private[streaming] override def name: String = s"Loghub direct stream [$id]"

  def tryToCreateConsumerGroup(): Unit = {
    try {
      loghubClient
        .CreateConsumerGroup(project, logStore, new ConsumerGroup(consumerGroup, 10, true))
      savedCheckpoints = new mutable.HashMap[Int, String]()
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
            savedCheckpoints = fetchAllCheckpoints()
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

  def findCheckpointOrCursorForShard(shardId: Int,
                                     checkpoints: mutable.Map[Int, String]): String = {
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
    val initialCursor = cursor.GetCursor()
    logInfo(s"Start reading shard $shardId from $initialCursor")
    initialCursor
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
