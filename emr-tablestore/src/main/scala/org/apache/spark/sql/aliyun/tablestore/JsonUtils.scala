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

import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.collection.mutable
import scala.util.control.NonFatal

private object JsonUtils {
  private implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  /**
   * Write per-TunnelChannel offsets as json string.
   */
  def channelOffsets(channelOffsets: Map[TunnelChannel, ChannelOffset]): String = {
    var result = new mutable.HashMap[String, mutable.HashMap[String, ChannelOffset]]
    implicit val channelOrdering: Ordering[TunnelChannel] = Ordering.by(tc => (tc.tableName, tc.tunnelId, tc.channelId))
    val channels = channelOffsets.keySet.toSeq.sorted // sort for more determinism
    channels.foreach { c =>
      val off = channelOffsets(c)
      val parts = result.getOrElse(
        Tunnel(c.tableName, c.tunnelId).toString,
        new mutable.HashMap[String, ChannelOffset]
      )
      parts += c.channelId -> off
      result += Tunnel(c.tableName, c.tunnelId).toString -> parts
    }
    Serialization.write(result)
  }

  def channelOffsets(str: String): Map[TunnelChannel, ChannelOffset] = {
    try {
      Serialization.read[Map[String, Map[String, ChannelOffset]]](str).flatMap {
        case (tunnel, tunnelOffset) =>
          tunnelOffset.map {
            case (channelId, offset) =>
              val tableName = tunnel.split("#")(0)
              val tunnelName = tunnel.split("#")(1)
              TunnelChannel(tableName, tunnelName, channelId) -> offset
          }
      }
    } catch {
      case NonFatal(_) =>
        throw new IllegalArgumentException(
          s"""Expected
             |{
             |  "table-A#tunnel-A":{
             |    "channelId1":"(\"checkpoint1\", 0)",
             |    "channelId2":"(\"checkpoint2\", 6)
             |  }
             |}, got $str""")
    }
  }
}
