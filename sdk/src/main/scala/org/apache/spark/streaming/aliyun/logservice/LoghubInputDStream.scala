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
    loghubEndpoint: String,
    accessKeyId: String,
    accessKeySecret: String,
    storageLevel: StorageLevel)
  extends ReceiverInputDStream[Array[Byte]](_ssc){
  val mConsumeInOrder = _ssc.sc.getConf.getBoolean("spark.logservice.fetch.inOrder", true)
  val mHeartBeatIntervalMillis = _ssc.sc.getConf.getLong("spark.logservice.heartbeat.interval.millis", 30000L)
  val dataFetchIntervalMillis = _ssc.sc.getConf.getLong("spark.logservice.fetch.interval.millis", 200L)

  override def getReceiver(): Receiver[Array[Byte]] =
    new LoghubReceiver(
      mConsumeInOrder,
      mHeartBeatIntervalMillis,
      dataFetchIntervalMillis,
      logServiceProject,
      logStoreName,
      loghubConsumerGroupName,
      loghubInstanceNameBase,
      loghubEndpoint,
      accessKeyId,
      accessKeySecret,
      storageLevel)
}
