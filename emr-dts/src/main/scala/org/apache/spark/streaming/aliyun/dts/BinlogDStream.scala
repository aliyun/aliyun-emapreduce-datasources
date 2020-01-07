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
package org.apache.spark.streaming.aliyun.dts

import com.aliyun.drc.clusterclient.message.ClusterMessage

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private[dts] class BinlogDStream(
    @transient _ssc: StreamingContext,
    accessKeyId: String,
    accessKeySecret: String,
    val guid: String,
    func: ClusterMessage => String,
    storageLevel: StorageLevel,
    usePublicIp: Boolean = false)
  extends ReceiverInputDStream[String](_ssc) {
  // Check whether there are multiple BinlogDStream subscribing to dts guid.
  // NOTE: this must be placed at the beginning of the BinlogDStream constructor.
  BinlogDStream.warnOtherDtsReceiverIsRunning(this)
  override def getReceiver(): Receiver[String] =
    new BinlogReceiver(accessKeyId, accessKeySecret, guid, func, storageLevel, usePublicIp)
}

private[dts] object BinlogDStream extends Logging {
  val activeBinlogDStream = new scala.collection.mutable.HashMap[String, Int]()
  def warnOtherDtsReceiverIsRunning(s: BinlogDStream): Unit = {
    if (activeBinlogDStream.contains(s.guid)) {
      activeBinlogDStream(s.guid) += 1
      logWarning(s"${activeBinlogDStream(s.guid)} dts receiver are subscribing to dts " +
        s"guid ${s.guid}, only one receiver can fetch dts data. For more information, please " +
        s"visit https://help.aliyun.com/document_detail/26657.html")
    } else {
      activeBinlogDStream.put(s.guid, 1)
    }
  }
}
