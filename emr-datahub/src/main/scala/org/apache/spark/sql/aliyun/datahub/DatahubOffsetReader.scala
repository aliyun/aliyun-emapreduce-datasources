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
package org.apache.spark.sql.aliyun.datahub

import java.io.UnsupportedEncodingException
import java.util.concurrent.{Executors, ThreadFactory}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import com.aliyun.datahub.DatahubConfiguration
import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.aliyun.datahub.DatahubClientAgent
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}

class DatahubOffsetReader(readerOptions: Map[String, String]) extends Logging {
  val datahubReaderThread = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new UninterruptibleThread("Datahub Offset Reader") {
        override def run(): Unit = {
          r.run()
        }
      }
      t.setDaemon(true)
      t
    }
  })
  val execContext = ExecutionContext.fromExecutorService(datahubReaderThread)
  private def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  private val project = readerOptions.getOrElse("project",
    throw new MissingArgumentException("Missing datahub project (='project')."))
  private val topic = readerOptions.getOrElse("topic",
    throw new MissingArgumentException("Missing datahub topic (='topic')."))
  private val maxOffsetFetchAttempts = readerOptions.getOrElse("fetchOffset.numRetries", "3").toInt
  private val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  var datahubClient: DatahubClientAgent =
    DatahubOffsetReader.getOrCreateDatahubClient(readerOptions)

  private def withRetriesWithoutInterrupt(
      body: => Map[DatahubShard, Long]): Map[DatahubShard, Long] = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    synchronized {
      var result: Option[Map[DatahubShard, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
        && !Thread.currentThread().isInterrupted) {
        Thread.currentThread match {
          case ut: UninterruptibleThread =>
            ut.runUninterruptibly {
              try {
                result = Some(body)
              } catch {
                case NonFatal(e) =>
                  lastException = e
                  logWarning(s"Error in attempt $attempt getting datahub offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
                  DatahubOffsetReader.resetConsumer(readerOptions)
              }
            }
          case _ =>
            throw new IllegalStateException(
              "datahub client APIs must be executed on a o.a.spark.util.UninterruptibleThread")
        }
      }
      if (Thread.interrupted()) {
        throw new InterruptedException()
      }
      if (result.isEmpty) {
        assert(attempt > maxOffsetFetchAttempts)
        assert(lastException != null)
        throw lastException
      }
      result.get
    }
  }

  def fetchDatahubShard(): Set[DatahubShard] = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    datahubClient.listShards(project, topic).getShards().asScala
      .map(shard => DatahubShard(project, topic, shard.getShardId())).toSet
  }

  def fetchEarliestOffsets(): Map[DatahubShard, Long] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      fetchDatahubShard().map { case datahubShard =>
        val cursor =
          datahubClient.getCursor(project, topic, datahubShard.shardId, CursorType.OLDEST)
        (datahubShard, cursor.getSequence)
      }.toMap
    }
  }

  def fetchEarliestOffsets(newPartitions: Set[DatahubShard]): Map[DatahubShard, Long] = {
    runUninterruptibly {
      withRetriesWithoutInterrupt {
        val partitionOffsets = fetchDatahubShard().map { case datahubShard =>
          val cursor =
            datahubClient.getCursor(project, topic, datahubShard.shardId, CursorType.OLDEST)
          (datahubShard, cursor.getSequence)
        }.toMap

        partitionOffsets.filter(po => newPartitions.contains(po._1))
      }
    }
  }

  def fetchLatestOffsets(): Map[DatahubShard, Long] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      fetchDatahubShard().map { case datahubShard =>
        val cursor =
          datahubClient.getCursor(project, topic, datahubShard.shardId, CursorType.LATEST)
        (datahubShard, cursor.getSequence)
      }.toMap
    }
  }

  def fetchLatestOffsets(knownOffsets: Option[Map[DatahubShard, Long]]): Map[DatahubShard, Long] =
    runUninterruptibly {
      withRetriesWithoutInterrupt {
        val partitionOffsets = fetchDatahubShard().map { case datahubShard =>
          val cursor =
            datahubClient.getCursor(project, topic, datahubShard.shardId, CursorType.LATEST)
          (datahubShard, cursor.getSequence)
        }.toMap

        if (knownOffsets.isDefined) {
          val missingShards = knownOffsets.get.keys.toSeq.diff(partitionOffsets.keys.toSeq)
          if (missingShards.nonEmpty) {
            throw new IllegalStateException(
              s"Found some shard missing in latest state: $missingShards")
          }
        }

        partitionOffsets
      }
  }

  def close(): Unit = {
    datahubClient = null
    datahubReaderThread.shutdown()
  }
}

object DatahubOffsetReader extends Logging with Serializable {
  @transient private var DatahubClient: DatahubClientAgent = null
  @transient private var zkClient: ZkClient = null

  def getOrCreateZKClient(zkParams: Map[String, String]): ZkClient = {
    if (zkClient == null) {
      val zkConnect = zkParams.getOrElse("connect.address",
        throw new MissingArgumentException(
          "Missing 'zookeeper.connect.address' option when create datahub source."))
      val zkSessionTimeoutMs = zkParams.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
      val zkConnectionTimeoutMs =
        zkParams.getOrElse("zookeeper.connection.timeout.ms", zkSessionTimeoutMs.toString).toInt
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
            case e: UnsupportedEncodingException =>
              null
          }
        }
      })
    }

    zkClient
  }

  def getOrCreateDatahubClient(
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String): DatahubClientAgent = {
    if (DatahubClient == null) {
      DatahubClient = new DatahubClientAgent(new DatahubConfiguration(new AliyunAccount(accessKeyId,
        accessKeySecret), endpoint))
    }
    DatahubClient
  }

  def getOrCreateDatahubClient(sourceOptions: Map[String, String]): DatahubClientAgent = {
    val accessKeyId = sourceOptions.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    val accessKeySecret = sourceOptions.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    val endpoint = sourceOptions.getOrElse("endpoint",
      throw new MissingArgumentException("Missing endpoint (='endpoint')."))
    if (DatahubClient == null) {
      DatahubClient = new DatahubClientAgent(
        new DatahubConfiguration(new AliyunAccount(accessKeyId, accessKeySecret), endpoint))
    }
    DatahubClient
  }

  def resetConsumer(sourceOptions: Map[String, String]): Unit = synchronized {
    DatahubClient = null
    DatahubClient = getOrCreateDatahubClient(sourceOptions)
  }
}
