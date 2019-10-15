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

package org.apache.spark.sql.aliyun.tablestore

import java.util.concurrent.{Executors, ThreadFactory}

import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest
import com.alicloud.openservices.tablestore.model.tunnel.{ChannelStatus, DescribeTunnelRequest, ListTunnelRequest}
import com.alicloud.openservices.tablestore.{SyncClient, SyncClientInterface, TunnelClient, TunnelClientInterface}
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class TableStoreOffsetReader(readerOptions: Map[String, String])
  extends Logging {

  var tablestoreReaderThread =
    Executors.newSingleThreadExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new UninterruptibleThread("TableStore Offset Reader") {
          override def run(): Unit = {
            r.run()
          }
        }
        t.setDaemon(true)
        t
      }
    })

  val execContext = ExecutionContext.fromExecutorService(tablestoreReaderThread)

  /**
   * This method ensures that the closure is called in an [[UninterpretableThread]].
   */
  private[sql] def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread().isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  val tunnelClient: TunnelClientInterface = TableStoreOffsetReader.getOrCreateTunnelClient(readerOptions)

  private val accessKeyId = readerOptions.getOrElse(
    "access.key.id",
    throw new MissingArgumentException("Missing access key id (='access.key.id').")
  )
  private val accessKeySecret = readerOptions.getOrElse(
    "access.key.secret",
    throw new MissingArgumentException("Missing access key secret (='access.key.secret').")
  )
  private val endpoint = readerOptions.getOrElse(
    "endpoint",
    throw new MissingArgumentException("Missing log store endpoint (='endpoint').")
  )
  private val instanceName = readerOptions.getOrElse(
    "ots.instance",
    throw new MissingArgumentException("Missing TableStore instance (='ots.instance').")
  )

  val syncClient: SyncClientInterface =
    TableStoreOffsetReader.getOrCreateSyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)

  private val tableName = readerOptions.getOrElse(
    "ots.table",
    throw new MissingArgumentException("Missing TableStore table (='ots.table').")
  )

  private val tunnelId = readerOptions.getOrElse(
    "ots.tunnel",
    throw new MissingArgumentException("Missing TableStore tunnel (='ots.tunnel').")
  )

  private val clientName = readerOptions.getOrElse("ots.client", "spark-client")

  def fetchTunnelChannels(): Set[TunnelChannel] = {
    val listResp = tunnelClient.listTunnel(new ListTunnelRequest(tableName))
    var tunnelName = ""
    listResp.getTunnelInfos.foreach(tunnel =>
      if (tunnel.getTunnelId == tunnelId) {
        tunnelName = tunnel.getTunnelName
      }
    )
    val describeResp = tunnelClient.describeTunnel(new DescribeTunnelRequest(tableName, tunnelName))
    describeResp.getChannelInfos
      .filter(channel => channel.getChannelStatus == ChannelStatus.OPEN)
      .map(channel => TunnelChannel(tableName, tunnelId, channel.getChannelId))
      .toSet
  }

  def fetchOffsetsFromTunnel(): Map[TunnelChannel, ChannelOffset] = {
    fetchTunnelChannels()
      .map { tc =>
        val getCheckpointResp = tunnelClient.getCheckpoint(
          new GetCheckpointRequest(tc.tunnelId, clientName, tc.channelId)
        )
        (tc, ChannelOffset(getCheckpointResp.getCheckpoint, 0L))
      }
      .filter(co => co._2 != ChannelOffset.TERMINATED_CHANNEL_OFFSET)
      .toMap
  }

  // Fetch open channels and filter terminated channels.
  def fetchStartOffsets(): Map[TunnelChannel, ChannelOffset] = {
    runUninterruptibly {
      fetchOffsetsFromTunnel()
    }
  }

  def close(): Unit = {
    if (tunnelClient != null) runUninterruptibly {
      tunnelClient.shutdown()
    }
    tablestoreReaderThread.shutdown()
  }
}

object TableStoreOffsetReader extends Logging with Serializable {
  @transient private var tunnelClient: TunnelClientInterface = null
  @transient private var syncClient: SyncClientInterface = null

  def getOrCreateTunnelClient(endpoint: String, accessKeyId: String,
    accessKeySecret: String, instanceName: String): TunnelClientInterface = {
    if (tunnelClient == null) {
      logInfo("create new tunnelClient")
      tunnelClient = new TunnelClient(endpoint, accessKeyId, accessKeySecret, instanceName)
    }
    tunnelClient
  }

  def getOrCreateTunnelClient(sourceOptions: Map[String, String]): TunnelClientInterface = {
    if (tunnelClient == null) {
      val accessKeyId = sourceOptions.getOrElse(
        "access.key.id",
        throw new MissingArgumentException("Missing access key id (='access.key.id').")
      )
      val accessKeySecret = sourceOptions.getOrElse(
        "access.key.secret",
        throw new MissingArgumentException("Missing access key secret (='access.key.secret').")
      )
      val endpoint = sourceOptions.getOrElse(
        "endpoint",
        throw new MissingArgumentException("Missing TableStore endpoint (='endpoint').")
      )
      val instanceName = sourceOptions.getOrElse(
        "ots.instance",
        throw new MissingArgumentException("Missing TableStore instance (='instance').")
      )
      tunnelClient = new TunnelClient(endpoint, accessKeyId, accessKeySecret, instanceName)
    }
    tunnelClient
  }

  def getOrCreateSyncClient(endpoint: String, accessKeyId: String,
    accessKeySecret: String, instanceName: String): SyncClientInterface = {
    if (syncClient == null) {
      logInfo("crate new syncClient")
      syncClient = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)
    }
    syncClient
  }
}
