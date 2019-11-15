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

import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory}

import com.aliyun.openservices.aliyun.log.producer.{LogProducer, ProducerConfig, ProjectConfig}
import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.common.Histogram
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
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
  private var latestHistograms: util.ArrayList[Histogram] = null

  private val logProject = readerOptions.getOrElse("sls.project",
    throw new MissingArgumentException("Missing logService project (='sls.project')."))
  private val logStore = readerOptions.getOrElse("sls.store",
    throw new MissingArgumentException("Missing logService store (='sls.store')."))
  private val maxOffsetFetchAttempts = readerOptions.getOrElse("fetchOffset.numRetries", "3").toInt
  private val offsetFetchAttemptIntervalMs = readerOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  var logServiceClient: LoghubClientAgent = LoghubOffsetReader.getOrCreateLoghubClient(readerOptions)

  private def withRetriesWithoutInterrupt[T](body: => T): T = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    synchronized {
      var result: Option[T] = None
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
    logServiceClient.ListShard(logProject, logStore).GetShards()
      .map(shard => LoghubShard(logProject, logStore, shard.GetShardId())).toSet
  }

  def fetchEarliestOffsets(loghubShards: Set[LoghubShard]): Map[LoghubShard, (Int, String)] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      loghubShards.map(shard => {
        val cursor = logServiceClient.GetCursor(logProject, logStore, shard.shard, CursorMode.BEGIN)
        val cursorTime = logServiceClient.GetCursorTime(logProject, logStore, shard.shard, cursor.GetCursor())
        (shard, (cursorTime.GetCursorTime(), cursor.GetCursor()))
      }).toMap
    }
  }

  def fetchEarliestOffsets(): Map[LoghubShard, (Int, String)] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      fetchLoghubShard().map {case loghubShard =>
        val cursor = logServiceClient.GetCursor(logProject, logStore, loghubShard.shard, CursorMode.BEGIN)
        val cursorTime = logServiceClient.GetCursorTime(logProject, logStore, loghubShard.shard, cursor.GetCursor())
        (loghubShard, (cursorTime.GetCursorTime(), cursor.GetCursor()))
      }.toMap
    }
  }

  def fetchLatestOffsets(): Map[LoghubShard, (Int, String)] = runUninterruptibly {
    withRetriesWithoutInterrupt {
      fetchLoghubShard().map {case loghubShard => {
        val cursor = logServiceClient.GetCursor(logProject, logStore, loghubShard.shard, CursorMode.END)
        val cursorTime = logServiceClient.GetCursorTime(logProject, logStore, loghubShard.shard, cursor.GetCursor())
        (loghubShard, (cursorTime.GetCursorTime(), cursor.GetCursor()))
      }}.toMap
    }
  }

  private def getLatestHistograms(startOffset: Int): util.ArrayList[Histogram] = {
    val lag = System.currentTimeMillis() / 1000 - startOffset
    var tries = 10
    val maxRange = 60 * 5
    val minRange = 60
    val endOffset: Int = if (lag > maxRange) {
      startOffset + maxRange
    } else if (lag > minRange) {
      fetchLatestOffsets().values.map(_._1).min
    } else {
      throw new Exception("Should not be called here.")
    }

    var result = logServiceClient.GetHistograms(logProject, logStore, startOffset, endOffset, "", "*")
    while (!result.IsCompleted() && tries > 0) {
      result = logServiceClient.GetHistograms(logProject, logStore, startOffset, startOffset + maxRange, "", "*")
      tries -= 1
    }
    result.GetHistograms()
  }

  def rateLimit(startOffset: Int, maxOffsetsPerTrigger: Option[Long]): Int = {
    runUninterruptibly {
      withRetriesWithoutInterrupt {
        val lag = System.currentTimeMillis() / 1000 - startOffset
        if (lag <= 60) {
          val minCursorTime = fetchLatestOffsets().values.min
          require(minCursorTime._1 >= startOffset, s"endCursorTime[$minCursorTime] should not be less than " +
            s"startCursorTime[$startOffset].")
          return minCursorTime._1
        }

        if (latestHistograms == null) {
          latestHistograms = getLatestHistograms(startOffset)
        }

        val maxOffset = latestHistograms.map(_.mToTime).max
        if (startOffset >= maxOffset) {
          latestHistograms = getLatestHistograms(startOffset)
        }

        latestHistograms.map(_.mFromTime).reduceLeft((l, r) => {
          if (l >= r) {
            throw new Exception("Histograms should be ordered by time in second.")
          }
          r
        })

        import scala.collection.JavaConversions._
        var endCursorTime = startOffset
        var count = 0L
        latestHistograms.filter(_.mFromTime >= startOffset)
          .foreach(e => {
            if (count < maxOffsetsPerTrigger.get) {
              endCursorTime = e.mToTime
              count += e.mCount
            }
          })

        require(endCursorTime >= startOffset, s"endCursorTime[$endCursorTime] should not be less than " +
          s"startCursorTime[$startOffset].")
        endCursorTime
      }
    }
  }

  def close(): Unit = {
    logServiceClient = null
    loghubReaderThread.shutdown()
  }
}

object LoghubOffsetReader extends Logging with Serializable {
  @transient private var logProducer: LogProducer = null
  @transient private[logservice] var logServiceClientPool = new ConcurrentHashMap[(String, String), LoghubClientAgent]()

  def getOrCreateLoghubClient(
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String): LoghubClientAgent = {
    if (!logServiceClientPool.contains((accessKeyId, endpoint))) {
      val logServiceClient = new LoghubClientAgent(endpoint, accessKeyId, accessKeySecret)
      logServiceClientPool.put((accessKeyId, endpoint), logServiceClient)
    }
    logServiceClientPool.get((accessKeyId, endpoint))
  }

  def getOrCreateLoghubClient(sourceOptions: Map[String, String]): LoghubClientAgent = {
    val accessKeyId = sourceOptions.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    val accessKeySecret = sourceOptions.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    val endpoint = sourceOptions.getOrElse("endpoint",
      throw new MissingArgumentException("Missing log store endpoint (='endpoint')."))
    getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
  }

  def getOrCreateLogProducer(sourceOptions: Map[String, String]): LogProducer = {
    if (logProducer == null) {
      val logProject = sourceOptions.getOrElse("sls.project",
        throw new MissingArgumentException("Missing logService project (='sls.project')."))
      val accessKeyId = sourceOptions.getOrElse("access.key.id",
        throw new MissingArgumentException("Missing access key id (='access.key.id')."))
      val accessKeySecret = sourceOptions.getOrElse("access.key.secret",
        throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
      val endpoint = sourceOptions.getOrElse("endpoint",
        throw new MissingArgumentException("Missing log store endpoint (='endpoint')."))
      val config = new ProducerConfig()
      logProducer = new LogProducer(config)
      logProducer.putProjectConfig(new ProjectConfig(logProject, endpoint, accessKeyId, accessKeySecret))
    }
    logProducer
  }

  def resetConsumer(sourceOptions: Map[String, String]): Unit = synchronized {
    logServiceClient = null
    logServiceClient = getOrCreateLoghubClient(sourceOptions)
  }

  // just for test
  private[logservice] def setLogServiceClient(testClient: LoghubClientAgent): Unit = {
    this.logServiceClient = testClient
  }

  // just for test
  private[logservice] def resetLogServiceClient(): Unit = {
    logServiceClient = null
  }
}
