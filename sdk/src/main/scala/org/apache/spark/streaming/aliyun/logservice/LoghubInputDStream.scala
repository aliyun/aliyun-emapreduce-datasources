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

import com.aliyun.ms.MetaClient
import com.aliyun.ms.utils.EndpointEnum
import com.aliyun.openservices.log.Client
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class LoghubInputDStream(
    @transient _ssc: StreamingContext,
    logServiceProject: String,
    logStoreName: String,
    loghubConsumerGroupName: String,
    loghubInstanceNameBase: String,
    var loghubEndpoint: String,
    var accessKeyId: String,
    var accessKeySecret: String,
    storageLevel: StorageLevel,
    cursorPosition: LogHubCursorPosition,
    mLoghubCursorStartTime: Int,
    forceSpecial: Boolean)
  extends ReceiverInputDStream[Array[Byte]](_ssc){
  val mConsumeInOrder =
    _ssc.sc.getConf.getBoolean("spark.logservice.fetch.inOrder", true)
  val mHeartBeatIntervalMillis =
    _ssc.sc.getConf.getLong("spark.logservice.heartbeat.interval.millis", 30000L)
  val dataFetchIntervalMillis =
    _ssc.sc.getConf.getLong("spark.logservice.fetch.interval.millis", 200L)
  val batchInterval = _ssc.graph.batchDuration.milliseconds
  var securityToken: String = null
  @transient lazy val slsClient =
    if (accessKeyId == null || accessKeySecret == null) {
      accessKeyId = MetaClient.getRoleAccessKeyId
      accessKeySecret = MetaClient.getRoleAccessKeySecret
      securityToken = MetaClient.getRoleSecurityToken
      loghubEndpoint = if (loghubEndpoint == null) {
        val region = MetaClient.getClusterRegionName
        val nType = MetaClient.getClusterNetworkType
        val endpointBase = EndpointEnum.getEndpoint("log", region, nType)
        s"$logServiceProject.$endpointBase"
      } else {
        loghubEndpoint
      }
      val client = new Client(loghubEndpoint, accessKeyId, accessKeySecret)
      client.SetSecurityToken(securityToken)
      client
    } else {
      new Client(loghubEndpoint, accessKeyId, accessKeySecret)
    }

  if (forceSpecial && cursorPosition.toString.equals(
    LogHubCursorPosition.SPECIAL_TIMER_CURSOR.toString)) {
    try {
      slsClient.DeleteConsumerGroup(logServiceProject, logStoreName,
        loghubConsumerGroupName)
    } catch {
      case e: Exception =>
        // In case of expired token
        if (securityToken != null) {
          try {
            accessKeyId = MetaClient.getRoleAccessKeyId
            accessKeySecret = MetaClient.getRoleAccessKeySecret
            securityToken = MetaClient.getRoleSecurityToken
            val client = new Client(loghubEndpoint, accessKeyId, accessKeySecret)
            client.SetSecurityToken(securityToken)
            client.DeleteConsumerGroup(logServiceProject, logStoreName,
              loghubConsumerGroupName)
          } catch {
            case e: Exception =>
              logError(s"Failed to delete consumer group, ${e.getMessage}", e)
              throw e
          }
        } else {
          logError(s"Failed to delete consumer group, ${e.getMessage}", e)
          throw e
        }
    }
  }

  override def getReceiver(): Receiver[Array[Byte]] =
    new LoghubReceiver(
      mConsumeInOrder,
      mHeartBeatIntervalMillis,
      dataFetchIntervalMillis,
      batchInterval,
      logServiceProject,
      logStoreName,
      loghubConsumerGroupName,
      loghubInstanceNameBase,
      loghubEndpoint,
      accessKeyId,
      accessKeySecret,
      storageLevel,
      cursorPosition,
      mLoghubCursorStartTime)

  def this(@transient _ssc: StreamingContext,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubInstanceNameBase: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel) = {
    this(_ssc, logServiceProject, logStoreName, loghubConsumerGroupName,
      loghubInstanceNameBase, loghubEndpoint, accessKeyId, accessKeySecret,
      storageLevel, LogHubCursorPosition.END_CURSOR, -1, false)
  }
}
