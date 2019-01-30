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

import java.io.{IOException, ObjectInputStream, UnsupportedEncodingException}
import java.nio.charset.StandardCharsets

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConversions._
import scala.collection.mutable

import com.aliyun.datahub.DatahubConfiguration
import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.exception.InvalidParameterException
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.model.{GetCursorResult, OffsetContext, RecordEntry, ShardState}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.Utils

class DirectDatahubInputDStream(
    @transient _ssc: StreamingContext,
    endpoint: String,
    project: String,
    topic: String,
    subId: String,
    accessId: String,
    accessKey: String,
    func: RecordEntry => String,
    mode: CursorType,
    zkParam: Map[String, String]) extends InputDStream[Array[Byte]](_ssc) with CanCommitOffsets {
  @transient private var datahubClient: DatahubClientAgent = null
  @transient private var zkClient: ZkClient = null
  private var checkpointDir: String = null
  var datahubConsumePathRoot: String = null
  var datahubCommitPathRoot: String = null
  @transient private var restart = false
  @transient private var commitLock = new Object
  private var doCommit = false
  private var restartTime = -1L
  private var lastJobTime = -1L
  private var readOnlyShardCache = new mutable.HashMap[String, Long]()

  override def start(): Unit = {
    datahubClient = new DatahubClientAgent(new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint))
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
            logError("Fail to write data to Zookeeper.", e)
            null
        }
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        if (bytes == null) {
          return null
        }
        try {
          new String(bytes, StandardCharsets.UTF_8)
        } catch {
          case e: UnsupportedEncodingException =>
            logError("Fail to fetch data from Zookeeper.", e)
            null
        }
      }
    })

    checkpointDir = new Path(ssc.checkpointDir).toUri.getPath
    datahubConsumePathRoot = s"$checkpointDir/datahub/consume"
    datahubCommitPathRoot = s"$checkpointDir/datahub/commit"
    if (!zkClient.exists(s"$checkpointDir/datahub")) {
      zkClient.createPersistent(s"$datahubConsumePathRoot/$project/$topic",true)
      zkClient.createPersistent(s"$datahubCommitPathRoot/$project/$topic", true)
    }

    val pathExist = zkClient.exists(s"$datahubConsumePathRoot/$project/$topic/$subId")
    val initial = if(pathExist) {
      zkClient.getChildren(s"$datahubConsumePathRoot/$project/$topic/$subId")
        .toArray()
    } else Array.empty[String]
    val shards = datahubClient.listShards(project, topic).getShards

    val diff = shards.map(_.getShardId).diff(initial)
    diff.foreach(shardId => {
      val oldestCursor = fetchCursorFormDatahub(shardId)
      val oldestOffset = new OffsetContext.Offset(oldestCursor.getSequence, oldestCursor.getRecordTime)
      DirectDatahubInputDStream.writeDataToZk(zkClient,
        s"$datahubConsumePathRoot/$project/$topic/$subId/$shardId",
        JacksonParser.toJsonNode(oldestOffset).toString)
    })
  }

  override def stop(): Unit ={
    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }
  }

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    if (restartTime == -1) {
      val zeroTime = graph.zeroTime.milliseconds
      val gap = System.currentTimeMillis() -  zeroTime
      val duration = graph.batchDuration.milliseconds
      restartTime = (math.floor(gap / duration).toLong + 1) * duration + zeroTime
    }

    commitLock.synchronized {
      val lastFailed = if (doCommit) {
        false
      } else {
        val pending = ssc.scheduler.getPendingTimes().exists(time => time.milliseconds == lastJobTime)
        if (pending) {
          false
        } else {
          true
        }
      }

      val shardOffsets: ArrayBuffer[(String, String, String)] = new ArrayBuffer()
      val rdd = if (validTime.milliseconds > restartTime && (doCommit || lastFailed)) {
        if (restart || lastFailed) {
          restart = false
        } else {
          commitAll()
        }

        val result = datahubClient.listShards(project, topic)
        result.getShards.foreach(shardEntry => {
          val shardId = shardEntry.getShardId
          if (readOnlyShardCache.contains(shardId)) {
            logDebug(s"Get no record in read only shard[$shardId], do nothing")
          } else {
            var startOffset: OffsetContext.Offset = null
            try {
              val consume: String = zkClient.readData(s"$datahubConsumePathRoot/$project/$topic/$subId/$shardId")
              startOffset = JacksonParser.getOffset(consume)
            } catch {
              case _:ZkNoNodeException =>
                logWarning("Fail to get start offset form zookeeper, retry with datahub")
                val cursorResult = fetchCursorFormDatahub(shardId)
                startOffset = new OffsetContext.Offset(cursorResult.getSequence, cursorResult.getRecordTime)
            }

            val latestCursor = datahubClient.getCursor(project, topic, shardId, CursorType.LATEST)
            val endOffset = new OffsetContext.Offset(latestCursor.getSequence, latestCursor.getRecordTime)
            if (startOffset.getSequence <= endOffset.getSequence) {
              val so = (shardId, JacksonParser.toJsonNode(startOffset).toString, JacksonParser.toJsonNode(endOffset).toString)
              shardOffsets += so
            }
            if (ShardState.CLOSED.equals(shardEntry.getState) && latestCursor.getSequence >= endOffset.getSequence) {
              readOnlyShardCache.put(shardId, endOffset.getSequence)
            }
          }
        })

        lastJobTime = validTime.milliseconds
        new DatahubRDD(_ssc.sc, _ssc.graph.batchDuration.milliseconds, endpoint, project, topic, subId, accessId,
          accessKey, func, shardOffsets, zkParam, checkpointDir)
      } else {
        return None
      }

      val description = shardOffsets.map { p =>
        val offset = "offset: [ %1$-30s to %2$-30s ]".format(p._2, p._3)
        s"shardId: ${p._1}\t $offset"
      }.mkString("\n")
      val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> description)
      if (storageLevel != StorageLevel.NONE && rdd.isInstanceOf[DatahubRDD]) {
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

  override private[streaming] def name = s"datahub direct stream[$id]"

  override def commitAsync(): Unit = {
    commitLock.synchronized {
      doCommit = true
    }
  }

  private def commitAll(): Unit = {
    if (doCommit) {
      try {
        val shards = zkClient.getChildren(s"$datahubCommitPathRoot/$project/$topic/$subId")
        shards.foreach(shard => {
          if (!readOnlyShardCache.contains(shard)) {
            val commit: String = zkClient.readData(s"$datahubCommitPathRoot/$project/$topic/$subId/$shard")
            val offset = JacksonParser.getOffset(commit)
            val currentOffsetContext = datahubClient.initOffsetContext(project, topic, subId, shard)
            currentOffsetContext.setOffset(offset)
            datahubClient.commitOffset(currentOffsetContext)
            DirectDatahubInputDStream.writeDataToZk(zkClient,
              s"$datahubConsumePathRoot/$project/$topic/$subId/$shard",
              JacksonParser.toJsonNode(offset).toString)
          }
        })
        doCommit = false
      } catch {
        case _: ZkNoNodeException => {
          logWarning("If this is the first time to run, it is fine to not find any commit data in " +
            "zookeeper.")
          doCommit = false
        }
      }
    }
  }

  private def fetchCursorFormDatahub(shardId: String): GetCursorResult = {
    val offsetContext = datahubClient.initOffsetContext(project, topic, subId, shardId)
    if (offsetContext.hasOffset) {
      try {
        datahubClient.getNextOffsetCursor(offsetContext)
      } catch {
        case e: InvalidParameterException =>
          if (!isDataExpired(shardId)) {
            throw e
          }

          logInfo("Fail to get cursor from offset context, cause data is expired. Retry with latest cursor")
          datahubClient.getCursor(project, topic, shardId, CursorType.OLDEST)
        case e: Exception =>
          logInfo(s"catch exception in fetchCursorFormDatahub, type:${e.getClass}")
          throw e
      }
    } else {
      mode match {
        case CursorType.OLDEST => datahubClient.getCursor(project, topic, shardId, CursorType.OLDEST)
        case CursorType.LATEST => datahubClient.getCursor(project, topic, shardId, CursorType.LATEST)
        case CursorType.SEQUENCE => datahubClient.getCursor(project, topic, shardId, CursorType.SEQUENCE)
        case CursorType.SYSTEM_TIME => datahubClient.getCursor(project, topic, shardId, CursorType.SYSTEM_TIME)
      }
    }
  }

  private def isDataExpired(shardId: String): Boolean = {
    val oldestDataTimestamp = datahubClient.getCursor(project, topic, shardId, CursorType.OLDEST).getRecordTime
    val offsetContextTimestamp = datahubClient.initOffsetContext(project, topic, subId, shardId)
      .getOffset.getTimestamp
    oldestDataTimestamp > offsetContextTimestamp
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    // TODO: move following logic to proper restart code path.
    this.synchronized {
      logDebug(s"${this.getClass.getSimpleName}.readObject used")
      ois.defaultReadObject()
      generatedRDDs = new HashMap[Time, RDD[Array[Byte]]]()
      readOnlyShardCache = new mutable.HashMap[String, Long]()
      commitLock = new Object()
      val zkServers = zkParam.getOrElse("zookeeper.connect", "localhost:2181")
      val sessionTimeout = zkParam.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
      val connectionTimeout = zkParam.getOrElse("zookeeper.connection.timeout.ms", sessionTimeout.toString).toInt
      zkClient = new ZkClient(zkServers, sessionTimeout, connectionTimeout)
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

      datahubClient = new DatahubClientAgent(new DatahubConfiguration(new AliyunAccount(accessId, accessKey), endpoint))
      restartTime = -1L
      restart = true
    }
  }

  private class DirectDatahubInputDStreamCheckpointData extends DStreamCheckpointData(this) {
    override def update(time: Time): Unit = {}

    override def cleanup(time: Time): Unit = {}

    override def restore(): Unit = {}

    override def finalize(): Unit = {
      super.finalize()
      stop()
    }
  }
}

object DirectDatahubInputDStream {
  private def writeDataToZk(zkClient: ZkClient, path: String, data: String) = {
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true)
    }
    zkClient.writeData(path, data)
  }
}