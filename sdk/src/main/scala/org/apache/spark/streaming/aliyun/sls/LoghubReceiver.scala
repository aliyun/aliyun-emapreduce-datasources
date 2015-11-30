/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.spark.streaming.aliyun.sls

import com.aliyun.openservices.loghub.client.ClientWorker
import com.aliyun.openservices.loghub.client.config.{LogHubConfig, LogHubCursorPosition, LogHubClientDbConfig}
import com.aliyun.openservices.loghub.client.lease.impl.MySqlLogHubLeaseManager
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

private[sls] class LoghubReceiver(
    mysqlHost: String,
    mysqlPort: Int,
    mysqlDatabase: String,
    mysqlUser: String,
    mysqlPwd: String,
    mysqlWorkerInstanceTableName: String,
    mysqlShardLeaseTableName: String,
    loghubProject: String,
    logStream: String,
    loghubConsumeGroup: String,
    instanceBaseName: String,
    loghubEndpoint: String,
    accesskeyId: String,
    accessKeySecret: String,
    storageLevel: StorageLevel)
  extends Receiver[Array[Byte]](storageLevel) with Logging {
  receiver =>

  private var workerThread: Thread = null
  private var worker: ClientWorker = null

  override def onStart(): Unit = {
    workerThread = new Thread() {
      override def run(): Unit = {
        val dbConfig = new LogHubClientDbConfig(mysqlHost, mysqlPort, mysqlDatabase, mysqlUser, mysqlPwd,
          mysqlWorkerInstanceTableName, mysqlShardLeaseTableName)
        val initCursor = LogHubCursorPosition.END_CURSOR

        val config = new LogHubConfig(loghubConsumeGroup, s"$instanceBaseName-$streamId",
          loghubEndpoint, loghubProject, logStream, accesskeyId, accessKeySecret, initCursor)

        config.setDataFetchIntervalMillis(1000)

        val leaseManager = new MySqlLogHubLeaseManager(dbConfig)

        worker = new ClientWorker(new SimpleLogHubProcessorFactory(receiver), config, leaseManager)

        worker.run()
      }
    }

    workerThread.setName(s"SLS Loghub Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()
    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    if (workerThread != null) {
      if (worker != null) {
        worker.shutdown()
      }

      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }

}
