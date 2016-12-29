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
package org.apache.spark.streaming.aliyun.mns.pulling

import java.util

import com.aliyun.mns.model.Message
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.mns.adapter.{CloudQueueAgent, MNSAgentUtil, MNSClientAgent}
import org.apache.spark.streaming.receiver.Receiver

@SerialVersionUID(3559189311748262112L)
private[mns] class MnsPullingReceiver(
    queueName: String,
    batchMsgSize: Int,
    pollingWaitSeconds: Int,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    storageLevel: StorageLevel,
    runLocal: Boolean,
    asRawByte: Boolean)
  extends Receiver[Array[Byte]](storageLevel) with Logging {
  receiver =>

  @transient private var workerThread: Thread = null
  @transient private var queue: CloudQueueAgent = null
  private val receiptsToDelete = new util.ArrayList[String]()

  override def onStart(): Unit = {
    queue = MnsPullingReceiver
        .getClient(accessKeyId, accessKeySecret, endpoint, runLocal)
        .getQueueRef(queueName)
    val func: Message => Array[Byte] = if (asRawByte) {
      message => message.getMessageBodyAsRawBytes
    } else {
      message => message.getMessageBodyAsBytes
    }

    workerThread = new Thread() {
      override def run(): Unit = {
        while (true) {
          try {
            val batchPopMessage =
              queue.batchPopMessage(batchMsgSize, pollingWaitSeconds)
            import scala.collection.JavaConversions._
            if (batchPopMessage == null) {
              log.warn("batch get nothing, wait for 5 seconds.")
              Thread.sleep(5000L)
            } else {
              for (popMsg <- batchPopMessage) {
                receiver.store(func(popMsg))
                receiptsToDelete.add(popMsg.getReceiptHandle)
              }
              queue.batchDeleteMessage(receiptsToDelete)
              receiptsToDelete.clear()
            }
          } catch {
            case ex: Throwable =>
              log.error(s"[MnsPullingReceiver Error]", ex)
              throw ex
          } finally {
            // Delete received message whatever.
            try {
              if (receiptsToDelete != null && receiptsToDelete.size() > 0) {
                queue.batchDeleteMessage(receiptsToDelete)
                receiptsToDelete.clear()
              }
            } catch {
              case e: Exception =>
                log.error(s"[Error] Failed to delete message, try again.", e)
                if (receiptsToDelete != null && receiptsToDelete.size() > 0) {
                  queue.batchDeleteMessage(receiptsToDelete)
                  receiptsToDelete.clear()
                }
            }
          }
        }
      }
    }

    workerThread.setName(s"MNS Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()
    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    // Delete received message whatever.
    queue.batchDeleteMessage(receiptsToDelete)

    if (workerThread != null) {
      MnsPullingReceiver.client.synchronized {
        var client = MnsPullingReceiver
            .getClient(accessKeyId, accessKeySecret, endpoint, runLocal)
        if (client != null && client.isOpen) {
          client.close()
          client = null
          Thread.sleep(5 * 1000)
        }
      }

      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }
}

private[mns] object MnsPullingReceiver extends Logging {
  private var client: MNSClientAgent = _

  def getClient(accessKeyId: String,
                accessKeySecret: String,
                endpoint: String,
                runLocal: Boolean): MNSClientAgent = {
    if (client == null) {
      try {
        client = MNSAgentUtil
            .getMNSClientAgent(accessKeyId, accessKeySecret, endpoint, runLocal)
      } catch {
        case e: Exception =>
          throw new RuntimeException("can not initialize mns client", e)
      }
    }

    client
  }

  def apply(
      queueName: String,
      batchMsgSize: Int,
      pollingWaitSeconds: Int,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      storageLevel: StorageLevel,
      runLocal: Boolean,
      asRawByte: Boolean): MnsPullingReceiver = {
    new MnsPullingReceiver(queueName, batchMsgSize, pollingWaitSeconds,
      accessKeyId, accessKeySecret, endpoint, storageLevel, runLocal, asRawByte)
  }
}
