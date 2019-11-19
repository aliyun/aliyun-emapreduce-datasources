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

import java.util

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.common.LogGroupData
import com.aliyun.openservices.log.common.Logs.Log
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor

import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._

class SimpleLogHubProcessor(receiver: LoghubReceiver)
    extends ILogHubProcessor with Logging {
  private var mShardId: Int = 0
  private var mLastCheckTime = 0L

  override def shutdown(iLogHubCheckPointTracker: ILogHubCheckPointTracker): Unit = {
    iLogHubCheckPointTracker.saveCheckPoint(true)
  }

  override def initialize(mShardId: Int): Unit = {
    this.mShardId = mShardId
  }

  override def process(list: util.List[LogGroupData],
      iLogHubCheckPointTracker: ILogHubCheckPointTracker): String = {
    // scalastyle:off
    import scala.collection.JavaConversions._
    // scalastyle:on
    try {
      list.foreach(group => {
        group.GetLogGroup().getLogsList.foreach(log => {
          process(group, log)
        })
      })
      val ct = System.currentTimeMillis()
      (ct - mLastCheckTime) > receiver.getBatchInterval match {
        case true =>
          iLogHubCheckPointTracker.saveCheckPoint(true)
          mLastCheckTime = ct
        case false =>
          iLogHubCheckPointTracker.saveCheckPoint(false)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    ""
  }

  private def process(group: LogGroupData, log: Log): Unit = {
    // scalastyle:off
    import scala.collection.JavaConversions._
    // scalastyle:on
    try {
      val topic = group.GetTopic()
      val source = group.GetSource()
      val obj = new JSONObject()
      obj.put(__TIME__, Integer.valueOf(log.getTime))
      obj.put(__TOPIC__, topic)
      obj.put(__SOURCE__, source)
      log.getContentsList.foreach(content => {
        obj.put(content.getKey, content.getValue)
      })

      val flg = group.GetFastLogGroup()
      for (i <- 0 until flg.getLogTagsCount) {
        obj.put("__tag__:".concat(flg.getLogTags(i).getKey), flg.getLogTags(i).getValue)
      }

      receiver.store(obj.toJSONString.getBytes)
    } catch {
      case e: Exception =>
        logError("Failed to store data.", e)
        throw e
    }
  }
}
