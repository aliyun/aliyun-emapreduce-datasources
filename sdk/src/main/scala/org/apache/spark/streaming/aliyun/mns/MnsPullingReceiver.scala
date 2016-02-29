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
package org.apache.spark.streaming.aliyun.mns

import java.util

import com.aliyun.mns.client.{CloudQueue, CloudAccount, MNSClient}
import com.aliyun.mns.common.{ClientException, ServiceException}
import com.aliyun.mns.model.Message
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

private[mns] class MnsPullingReceiver(
    queueName: String,
    batchMsgSize: Int,
    pollingWaitSeconds: Int,
    func: Message => Array[Byte],
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    storageLevel: StorageLevel)
  extends Receiver[Array[Byte]](storageLevel) with Logging {
  receiver =>

  private var workerThread: Thread = null
  private var client: MNSClient = null
  private var queue: CloudQueue = null
  private val receiptsToDelete = new util.ArrayList[String]

  override def onStart(): Unit = {
    val account: CloudAccount = new CloudAccount(accessKeyId, accessKeySecret, endpoint)
    client = account.getMNSClient
    queue = client.getQueueRef(queueName)

    workerThread = new Thread() {
      override def run(): Unit = {
        try {
          while (true) {
            val batchPopMessage = queue.batchPopMessage(batchMsgSize, pollingWaitSeconds)
            import scala.collection.JavaConversions._
            for (popMsg <- batchPopMessage) {
              receiver.store(func(popMsg))
              receiptsToDelete.add(popMsg.getReceiptHandle)
            }
            queue.batchDeleteMessage(receiptsToDelete)
            receiptsToDelete.clear()
          }
        } catch {
          case sex: ServiceException =>
            log.error(s"[MNS Service Error]", sex)
            throw new Exception(sex)
          case cex: ClientException =>
            log.error(s"[MNS Client Error]", cex)
            throw new Exception(cex)
          case ex: Throwable =>
            log.error(s"[Error]", ex)
            throw new Exception(ex)
        } finally {
          // Delete received message whatever.
          queue.batchDeleteMessage(receiptsToDelete)
          if (client != null) {
            client.close()
          }
        }
      }
    }

    workerThread.setName(s"SLS Loghub Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()
    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    // Delete received message whatever.
    queue.batchDeleteMessage(receiptsToDelete)

    if (workerThread != null) {
      if (client != null && client.isOpen) {
        client.close()
        client = null
        Thread.sleep(5 * 1000)
      }

      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }
}
