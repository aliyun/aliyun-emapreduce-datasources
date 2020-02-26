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

import java.{util => ju}
import java.util.concurrent.locks.ReentrantLock

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

import collection.mutable._
import com.aliyun.datahub.client.DatahubClient
import com.aliyun.datahub.client.exception.DatahubClientException
import com.aliyun.datahub.client.model._


class DatahubAsyncDataWriter(
  client: DatahubClient,
  project: String,
  topic: String,
  batchSize: Int,
  batchNum: Int) {

  final val RECORDS_SIZE_PER_BATCH: Int = batchSize
  final val flushInterval: Long = 10000L  // 10 seconds

  // for the Future Queue
  final val BATCH_NUM = batchNum
  final val futureLock = new ReentrantLock
  final val notFull = futureLock.newCondition
  final val notEmpty = futureLock.newCondition
  val futuresQueue = new ju.ArrayList[Future[PutRecordsResult]]()

  // for the PutRecordsResult Queue
  val resultLock = new ReentrantLock
  val resultQueue = new ju.ArrayList[PutRecordsResult]()
  @volatile var failedWrite: Exception = _

  // batch buffer
  val buffer = new ju.concurrent.LinkedBlockingDeque[RecordEntry](RECORDS_SIZE_PER_BATCH)
  var lastWrite = System.currentTimeMillis

  startTimerFlush()

  def putRecord(record: RecordEntry): Unit = {
    buffer.put(record)
    if (buffer.size() >= RECORDS_SIZE_PER_BATCH) {
      sendBatch()
    }
  }

  def flush(): Unit = {
    if (buffer.size() >= 0) {
      sendBatch()
    }
  }

  def checkStatus(): Unit = {
    if (failedWrite != null) {
      return
    }

    resultLock.lockInterruptibly()

    resultQueue.asScala.foreach({ res =>
      if (res.getFailedRecordCount > 0) {
        var errList = new ListBuffer[String]()
        res.getPutErrorEntries.asScala.foreach(err => {
          errList += ("ErrCode:" + err.getErrorcode
            + "Index:" + err.getIndex
            + "ErrMsg" + err.getMessage)
        })
        failedWrite = new DatahubClientException(errList.mkString(""))
      }
    })

    resultLock.unlock()
  }

  def waitCompleted(): Unit = {
    futureLock.lockInterruptibly()
    try {
      while ( {
        futuresQueue.size > 0
      }) notFull.await()
    } finally {
      futureLock.unlock()
    }
  }

  private def sendBatch(): Unit = {
    val records = new ju.ArrayList[RecordEntry]()
    buffer.drainTo(records)
    if (records.size() == 0) {
      return
    }
    val f = asyncPutRecordsPack(records)
    putFuture(f)
    f.onComplete {
      case Success(result) =>
        removeFuture(f)
        putResult(result)
      case Failure(e) =>
        removeFuture(f)
        failedWrite = new Exception(e.getMessage)
    }
  }

  private def asyncPutRecordsPack(records: ju.ArrayList[RecordEntry])
              : Future[PutRecordsResult] = Future {
    lastWrite = System.currentTimeMillis
    client.putRecords(project, topic, records)
  }

  private def putResult(r: PutRecordsResult) = {
    resultLock.lock()
    resultQueue.add(r)
    resultLock.unlock()
  }

  private def putFuture(f: Future[PutRecordsResult]) = {
    futureLock.lockInterruptibly()
    try {
      while ( {
        futuresQueue.size >= BATCH_NUM
      }) notFull.await()

      futuresQueue.add(f)
      notEmpty.signal()
    } finally {
      futureLock.unlock()
    }
  }

  private def removeFuture(f: Future[PutRecordsResult]) = {
    futureLock.lockInterruptibly()
    try {
      futuresQueue.remove(f)
      notFull.signalAll()
    } finally {
      futureLock.unlock()
    }
  }

  private def startTimerFlush(): Unit = {
    val flusher = new ju.Timer("datahub.buffer.timer.flusher")
    flusher.schedule(new ju.TimerTask() {
      override def run(): Unit = {
        if ((System.currentTimeMillis - lastWrite > flushInterval) && buffer.size() > 0) {
          flush()
        }
      }
    }, flushInterval, flushInterval)
  }
}
