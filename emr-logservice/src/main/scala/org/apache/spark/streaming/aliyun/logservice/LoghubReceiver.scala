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

import com.aliyun.openservices.loghub.client.config.{LogHubConfig, LogHubCursorPosition}

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

private[logservice] class LoghubReceiver(
    mConsumeInOrder: Boolean,
    mHeartBeatIntervalMillis: Long,
    dataFetchIntervalMillis: Long,
    batchInterval: Long,
    logServiceProject: String,
    logStoreName: String,
    loghubConsumerGroupName: String,
    loghubInstanceNameBase: String,
    loghubEndpoint: String,
    accessKeyId: String,
    accessKeySecret: String,
    storageLevel: StorageLevel,
    cursorPosition: LogHubCursorPosition,
    mLoghubCursorStartTime: Int)
  extends Receiver[Array[Byte]](storageLevel) with Logging {
  receiver =>

  private var workerThread: Thread = null
  private var worker: ClientWorker = null

  override def onStart(): Unit = {
    val initCursor = cursorPosition
    val config = if (!cursorPosition.toString.equals(
      LogHubCursorPosition.SPECIAL_TIMER_CURSOR.toString)) {
      new LogHubConfig(loghubConsumerGroupName,
        s"$loghubConsumerGroupName-$loghubInstanceNameBase-$streamId",
        loghubEndpoint, logServiceProject, logStoreName, accessKeyId,
        accessKeySecret, initCursor, mHeartBeatIntervalMillis, mConsumeInOrder)
    } else {
      new LogHubConfig(loghubConsumerGroupName,
        s"$loghubConsumerGroupName-$loghubInstanceNameBase-$streamId",
        loghubEndpoint, logServiceProject, logStoreName, accessKeyId,
        accessKeySecret, mLoghubCursorStartTime, mHeartBeatIntervalMillis,
        mConsumeInOrder)
    }
    config.setDataFetchIntervalMillis(dataFetchIntervalMillis)

    worker = new ClientWorker(new SimpleLogHubProcessorFactory(receiver), config)

    workerThread = new Thread(worker)
    workerThread.setName(s"SLS Loghub Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()
    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    if (workerThread != null) {
      if (worker != null) {
        worker.shutdown()
        Thread.sleep(30 * 1000)
      }

      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }

  def getBatchInterval: Long = this.batchInterval
}
