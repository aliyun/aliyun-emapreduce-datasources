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

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor
import com.aliyun.openservices.sls.common.{LogItem, LogGroupData}

import scala.collection.JavaConversions._

class SimpleLogHubProcessor(receiver: LoghubReceiver) extends ILogHubProcessor {
  private var mShardId: String = _
  private var mLastCheckTime = 0L

  override def shutdown(iLogHubCheckPointTracker: ILogHubCheckPointTracker): Unit = {
    iLogHubCheckPointTracker.saveCheckPoint(true)
  }

  override def initialize(s: String): Unit = {
    mShardId = s
  }

  override def process(list: util.List[LogGroupData], iLogHubCheckPointTracker: ILogHubCheckPointTracker): String = {
    list.foreach(group => group.GetAllLogs().foreach(item => process(item)))
    val ct = System.currentTimeMillis()
    try {
      (ct - mLastCheckTime) > 60 * 1000 match {
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

  private def process(item: LogItem): Unit = {
    receiver.store(item.ToJsonString().getBytes)
  }
}
