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
package org.apache.spark.sql.aliyun.logservice

import java.io.UnsupportedEncodingException
import java.util.Base64
import java.util.concurrent.{Executors, ThreadFactory}

import com.aliyun.openservices.log.common.Consts.CursorMode
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.sql.types._
import org.apache.spark.streaming.aliyun.logservice.LoghubClientAgent
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class LoghubOffsetReader(readerOptions: Map[String, String]) extends Logging {
  val loghubReaderThread = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new UninterruptibleThread("Loghub Offset Reader") {
        override def run(): Unit = {
          r.run()
        }
      }
      t.setDaemon(true)
      t
    }
  })
  val execContext = ExecutionContext.fromExecutorService(loghubReaderThread)
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

  private val logProject = readerOptions.getOrElse("sls.project",
    throw new MissingArgumentException("Missing logService project (='sls.project')."))
  private val logStore = readerOptions.getOrElse("sls.store",
    throw new MissingArgumentException("Missing logService store (='sls.store')."))
  private val maxOffsetFetchAttempts = readerOptions.getOrElse("fetchOffset.numRetries", "3").toInt
  private val offsetFetchAttemptIntervalMs = readerOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  var logServiceClient: LoghubClientAgent = LoghubOffsetReader.getOrCreateLoghubClient(readerOptions)

  private def withRetriesWithoutInterrupt(
      body: => Map[LoghubShard, String]): Map[LoghubShard, String] = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    synchronized {
      var result: Option[Map[LoghubShard, String]] = None
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
                  logWarning(s"Error in attempt $attempt getting loghub offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
                  LoghubOffsetReader.resetConsumer(readerOptions)
              }
            }
          case _ =>
            throw new IllegalStateException(
              "Loghub client APIs must be executed on a o.a.spark.util.UninterruptibleThread")
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

  def fetchLoghubShard(): Set[LoghubShard] = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    logServiceClient.ListShard(logProject, logStore).GetShards()
      .map(shard => LoghubShard(logProject, logStore, shard.GetShardId())).toSet
  }

  def fetchEarliestOffsets(): Map[LoghubShard, String] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      fetchLoghubShard().map {case loghubShard => {
        val cursor = logServiceClient.GetCursor(logProject, logStore, loghubShard.shard, CursorMode.BEGIN)
        (loghubShard, cursor.GetCursor())
      }}.toMap
    }
  }

  def fetchLatestOffsets(): Map[LoghubShard, String] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      fetchLoghubShard().map {case loghubShard => {
        val cursor = logServiceClient.GetCursor(logProject, logStore, loghubShard.shard, CursorMode.END)
        (loghubShard, cursor.GetCursor())
      }}.toMap
    }
  }

  def close(): Unit = {
    logServiceClient = null
    loghubReaderThread.shutdown()
  }
}

object LoghubOffsetReader extends Logging with Serializable {
  @transient private var logServiceClient: LoghubClientAgent = null
  @transient private var zkClient: ZkClient = null

  def getOrCreateZKClient(zkParams: Map[String, String]): ZkClient = {
    if (zkClient == null) {
      val zkConnect = zkParams.getOrElse("connect.address",
        throw new MissingArgumentException("Missing 'zookeeper.connect.address' option when create loghub source."))
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

  def getOrCreateLoghubClient(
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String): LoghubClientAgent = {
    if (logServiceClient == null) {
      logServiceClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
    }
    logServiceClient
  }

  def getOrCreateLoghubClient(sourceOptions: Map[String, String]): LoghubClientAgent = {
    val accessKeyId = sourceOptions.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    val accessKeySecret = sourceOptions.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    val endpoint = sourceOptions.getOrElse("endpoint",
      throw new MissingArgumentException("Missing log store endpoint (='endpoint')."))
    if (logServiceClient == null) {
      logServiceClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
    }
    logServiceClient
  }

  def resetConsumer(sourceOptions: Map[String, String]): Unit = synchronized {
    logServiceClient = null
    logServiceClient = getOrCreateLoghubClient(sourceOptions)
  }

  def loghubSchema: StructType = {
    StructType(Seq(
      StructField("logProject", StringType),
      StructField("logStore", StringType),
      StructField("shardId", IntegerType),
      StructField("timestamp", TimestampType),
      StructField("value", BinaryType)
    ))
  }

  def loghubSchema(
      logProject: String,
      logStore: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String): StructType = {
    val logServiceClient = getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
    logInfo(s"Try to fetch one latest log to analyse underlying schema.")
    val (readOnlyShards, readAndWriteShards) = logServiceClient
      .ListShard(logProject, logStore).GetShards()
      .partition(s => s.getStatus.toLowerCase.equals("readonly"))
    val oneShard = if (readAndWriteShards.nonEmpty) {
      readAndWriteShards.head
    } else if (readOnlyShards.nonEmpty) {
      readOnlyShards.head
    } else {
      throw new IllegalStateException("There is no one log store shard in usable state.")
    }

    var schema = new StructType()
    val endCursor = logServiceClient.GetCursor(logProject, logStore, oneShard.GetShardId(), CursorMode.END).GetCursor()
    val endCursorDecoded = Base64.getDecoder.decode(endCursor)
    val startCursorLong = new String(endCursorDecoded).toLong - 1
    val startCursor = new String(Base64.getEncoder.encode(startCursorLong.toString.getBytes()))
    val oneBatchLogs = logServiceClient.BatchGetLog(logProject, logStore, oneShard.GetShardId(), 1, startCursor, endCursor)
    val group = oneBatchLogs.GetLogGroups().head
    val log = group.GetLogGroup().getLogsList.head

    schema = schema.add(StructField(__PROJECT__, StringType, false))
    schema = schema.add(StructField(__STORE__, StringType, false))
    schema = schema.add(StructField(__SHARD__, IntegerType, false))
    schema = schema.add(StructField(__TIME__, TimestampType, false))
    schema = schema.add(StructField(__TOPIC__, StringType, true))
    schema = schema.add(StructField(__SOURCE__, StringType, true))

    log.getContentsList.foreach(content => {
      schema = schema.add(StructField(content.getKey, StringType, true))
    })

    val flg = group.GetFastLogGroup()
    for (i <- 0 until flg.getLogTagsCount) {
      val tagKey = flg.getLogTags(i).getKey
      // exclude `__PACK_ID__` and `__USER_DEFINED_ID__` tag
      if (!tagKey.equals(__PACK_ID__) && !tagKey.equals(__USER_DEFINED_ID__)) {
        schema = schema.add(StructField(s"__tag$tagKey", StringType, true))
      }
    }

    schema
  }
}
