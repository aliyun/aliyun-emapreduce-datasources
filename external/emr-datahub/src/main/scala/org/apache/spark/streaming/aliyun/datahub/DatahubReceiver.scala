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

import com.aliyun.datahub.DatahubConfiguration
import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

private[datahub] class DatahubReceiver(
    projectName: String,
    topicName: String,
    subId: String,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    shardId: String,
    func: RecordEntry => String,
    storageLevel: StorageLevel) extends Receiver[Array[Byte]](storageLevel) with Logging {
  receiver =>

  private var workerThread: Thread = null
  private val conf = new DatahubConfiguration(new AliyunAccount(accessKeyId, accessKeySecret), endpoint)

  override def onStart(): Unit = {
    val dhbWorker = new DatahubWorker(projectName, topicName, shardId, subId, conf, receiver, func)
    workerThread = new Thread(dhbWorker)
    workerThread.setName(s"Datahub Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()
    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    if (workerThread != null) {
      workerThread.interrupt()
      workerThread.join(1000L)
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }
}
