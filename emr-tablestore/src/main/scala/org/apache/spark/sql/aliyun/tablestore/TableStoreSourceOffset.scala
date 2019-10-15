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

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset

case class TableStoreSourceOffset(channelOffsets: Map[TunnelChannel, ChannelOffset]) extends Offset {
  override val json: String = JsonUtils.channelOffsets(channelOffsets)
}

case class TableStoreSourceChannelOffset(channel: TunnelChannel, channelOffset: ChannelOffset) extends PartitionOffset

/** Companion object of the [[TableStoreSourceOffset]] */
private object TableStoreSourceOffset {
  def getChannelOffsets(offset: Offset): Map[TunnelChannel, ChannelOffset] = {
    offset match {
      case o: TableStoreSourceOffset => o.channelOffsets
      case so: SerializedOffset => TableStoreSourceOffset(so).channelOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to TableStoreSourceOffset"
        )
    }
  }

  def apply(offsetTuples: (String, String, String, ChannelOffset)*): TableStoreSourceOffset = {
    TableStoreSourceOffset(offsetTuples.map {
      case (table, tunnel, channel, offset) =>
        (TunnelChannel(table, tunnel, channel), offset)}.toMap)
  }

  def apply(offset: SerializedOffset): TableStoreSourceOffset =
    TableStoreSourceOffset(JsonUtils.channelOffsets(offset.json))

}
