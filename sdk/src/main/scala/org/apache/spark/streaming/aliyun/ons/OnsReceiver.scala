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
package org.apache.spark.streaming.aliyun.ons

import java.util.Properties

import com.aliyun.openservices.ons.api.Action
import com.aliyun.openservices.ons.api.ConsumeContext
import com.aliyun.openservices.ons.api.Consumer
import com.aliyun.openservices.ons.api.Message
import com.aliyun.openservices.ons.api.MessageListener
import com.aliyun.openservices.ons.api.PropertyKeyConst
import com.aliyun.openservices.ons.api.impl.ONSFactoryImpl
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

private[ons] class OnsReceiver(
    consumerID: String,
    topic: String,
    subExpression: String,
    accessKeyId: String,
    accessKeySecret: String,
    storageLevel: StorageLevel,
    func: Message => Array[Byte])
  extends Receiver[Array[Byte]](storageLevel) with Logging {
  receiver =>

  /** Thread running the worker */
  private var workerThread: Thread = null
  private var consumer: Consumer = null

  override def onStart() {
    workerThread = new Thread() {
      override def run(): Unit = {
        val properties = new Properties()
        properties.put(PropertyKeyConst.ConsumerId, consumerID)
        properties.put(PropertyKeyConst.AccessKey, accessKeyId)
        properties.put(PropertyKeyConst.SecretKey, accessKeySecret)
        val onsFactoryImpl = new ONSFactoryImpl
        consumer = onsFactoryImpl.createConsumer(properties)
        consumer.subscribe(topic, subExpression, new MessageListener() {
          override def consume(message: Message, context: ConsumeContext): Action = {
            try {
              receiver.store(func(message))
              Action.CommitMessage
            } catch {
              case e: Throwable =>
                Action.ReconsumeLater
            }
          }
        })

        consumer.start()
      }
    }

    workerThread.setName(s"Aliyun ONS Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()

    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    if (workerThread != null) {
      if (consumer != null) {
        consumer.shutdown()
      }

      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }
}
