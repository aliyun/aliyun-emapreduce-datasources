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

import com.alicloud.openservices.tablestore.SyncClientInterface
import com.alicloud.openservices.tablestore.model._
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable

class TableStoreCheckpointer(@transient syncClient: SyncClientInterface, tableName: String)
  extends Logging with Serializable {
  def checkpoint(channel: TunnelChannel, offset: ChannelOffset): Unit = {
    val pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    pkBuilder.addPrimaryKeyColumn("TunnelId", PrimaryKeyValue.fromString(channel.tunnelId))
    pkBuilder.addPrimaryKeyColumn("ChannelId", PrimaryKeyValue.fromString(channel.channelId))
    val rowPutChange = new RowPutChange(tableName, pkBuilder.build())
    rowPutChange.addColumn("LogPoint", ColumnValue.fromString(offset.logPoint))
    rowPutChange.addColumn("InnerOffset", ColumnValue.fromLong(offset.offset))
    logInfo(s"persist checkpoint, channel: ${channel}, offset: ${offset} ")
    syncClient.putRow(new PutRowRequest(rowPutChange))
  }

  def getCheckpoint(channel: TunnelChannel): ChannelOffset = {
    val pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    pkBuilder.addPrimaryKeyColumn("TunnelId", PrimaryKeyValue.fromString(channel.tunnelId))
    pkBuilder.addPrimaryKeyColumn("ChannelId", PrimaryKeyValue.fromString(channel.channelId))
    val criteria = new SingleRowQueryCriteria(tableName, pkBuilder.build())
    criteria.setMaxVersions(1)
    val row = syncClient.getRow(new GetRowRequest(criteria)).getRow
    val logPoint = row.getLatestColumn("LogPoint").getValue.asString
    val innerOffset = row.getLatestColumn("InnerOffset").getValue.asLong
    val offset = ChannelOffset(logPoint, innerOffset)
    logInfo(s"get persist checkpoint, channel: ${channel}, offset: ${offset}")
    offset
  }

  // After commit to Tunnel, delete finished checkpoint.
  def deleteCheckpoint(channel: TunnelChannel): Unit = {
    val pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    pkBuilder.addPrimaryKeyColumn("TunnelId", PrimaryKeyValue.fromString(channel.tunnelId))
    pkBuilder.addPrimaryKeyColumn("ChannelId", PrimaryKeyValue.fromString(channel.channelId))
    val rowDeleteChange = new RowDeleteChange(tableName, pkBuilder.build())
    syncClient.deleteRow(new DeleteRowRequest(rowDeleteChange))
    logInfo(s"delete finished persist checkpoint, channel: ${channel}")
  }

  def getTunnelCheckpoints(userTableName: String, tunnelId: String): Map[TunnelChannel, ChannelOffset] = {
    val rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName)
    val beginPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    beginPk.addPrimaryKeyColumn("TunnelId", PrimaryKeyValue.fromString(tunnelId))
    beginPk.addPrimaryKeyColumn("ChannelId", PrimaryKeyValue.INF_MIN)
    rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginPk.build())

    val endPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    endPk.addPrimaryKeyColumn("TunnelId", PrimaryKeyValue.fromString(tunnelId))
    endPk.addPrimaryKeyColumn("ChannelId", PrimaryKeyValue.INF_MAX)
    rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk.build())
    rangeRowQueryCriteria.setMaxVersions(1)

    val retMap = mutable.Map[TunnelChannel, ChannelOffset]()
    var isFinished = false
    while (!isFinished) {
      val getRangeResponse = syncClient.getRange(new GetRangeRequest(rangeRowQueryCriteria))
      getRangeResponse.getRows.foreach { row =>
        val channelId = row.getPrimaryKey.getPrimaryKeyColumn("ChannelId").getValue.asString
        val logPoint = row.getLatestColumn("LogPoint").getValue.asString
        val innerOffset = row.getLatestColumn("InnerOffset").getValue.asLong
        val offset = ChannelOffset(logPoint, innerOffset)
        retMap.put(TunnelChannel(userTableName, tunnelId, channelId), offset)
      }

      if (getRangeResponse.getNextStartPrimaryKey != null) {
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey)
      } else {
        isFinished = true
      }
    }
    retMap.toMap
  }

  // TODO: getCheckpoints via BatchGet
  def getCheckpoints(channels: List[TunnelChannel]): Map[TunnelChannel, ChannelOffset] = ???
}
