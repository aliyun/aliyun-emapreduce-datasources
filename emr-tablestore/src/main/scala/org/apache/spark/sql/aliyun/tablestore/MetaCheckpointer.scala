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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.alicloud.openservices.tablestore.SyncClientInterface
import com.alicloud.openservices.tablestore.model._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.tablestore.MetaCheckpointer._

class MetaCheckpointer(@transient syncClient: SyncClientInterface, tableName: String)
  extends Logging with Serializable {
  def checkpoint(channel: TunnelChannel, uuid: String, offset: ChannelOffset): Unit = {
    val pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    pkBuilder.addPrimaryKeyColumn(TUNNEL_ID_COLUMN, PrimaryKeyValue.fromString(channel.tunnelId))
    pkBuilder.addPrimaryKeyColumn(UUID_COLUMN, PrimaryKeyValue.fromString(uuid))
    pkBuilder.addPrimaryKeyColumn(CHANNEL_ID_COLUMN, PrimaryKeyValue.fromString(channel.channelId))
    val rowPutChange = new RowPutChange(tableName, pkBuilder.build())
    rowPutChange.addColumn(CHANNEL_OFFSET_COLUMN,
      ColumnValue.fromString(ChannelOffset.serialize(offset)))
    rowPutChange.addColumn(VERSION_COLUMN, ColumnValue.fromLong(CURRENT_VERSION))
    logInfo(s"persist checkpoint, channel: ${channel}, offset: ${offset} ")
    syncClient.putRow(new PutRowRequest(rowPutChange))
  }

  def checkpoint(uuid: String, channelOffsets: Map[TunnelChannel, ChannelOffset]): Unit = {
    logInfo(s"persist checkpoints: ${channelOffsets}")
    var idx = 0
    var batchRequest = new BatchWriteRowRequest
    channelOffsets.foreach { case (tc, offset) =>
      val pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
      pkBuilder.addPrimaryKeyColumn(TUNNEL_ID_COLUMN, PrimaryKeyValue.fromString(tc.tunnelId))
      pkBuilder.addPrimaryKeyColumn(UUID_COLUMN, PrimaryKeyValue.fromString(uuid))
      pkBuilder.addPrimaryKeyColumn(CHANNEL_ID_COLUMN, PrimaryKeyValue.fromString(tc.channelId))
      val rowPutChange = new RowPutChange(tableName, pkBuilder.build())
      rowPutChange.addColumn(CHANNEL_OFFSET_COLUMN,
        ColumnValue.fromString(ChannelOffset.serialize(offset)))
      rowPutChange.addColumn(VERSION_COLUMN, ColumnValue.fromLong(CURRENT_VERSION))
      batchRequest.addRowChange(rowPutChange)
      idx += 1
      if (idx == 200) {
        syncClient.batchWriteRow(batchRequest)
        idx = 0
        batchRequest = new BatchWriteRowRequest
      }
    }
    if (!batchRequest.isEmpty) {
      syncClient.batchWriteRow(batchRequest)
    }
  }

  // when channel is finished, delete from meta checkpoint table.
  def deleteCheckpoint(channel: TunnelChannel, batchUUID: String): Unit = {
    val pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    pkBuilder.addPrimaryKeyColumn(TUNNEL_ID_COLUMN, PrimaryKeyValue.fromString(channel.tunnelId))
    pkBuilder.addPrimaryKeyColumn(UUID_COLUMN, PrimaryKeyValue.fromString(batchUUID))
    pkBuilder.addPrimaryKeyColumn(CHANNEL_ID_COLUMN, PrimaryKeyValue.fromString(channel.channelId))
    val rowDeleteChange = new RowDeleteChange(tableName, pkBuilder.build())
    logInfo(s"delete meta checkpoint: ${channel}, batchUUID: ${batchUUID}")
    syncClient.deleteRow(new DeleteRowRequest(rowDeleteChange))
  }

  // whether has finished checkpoint
  def getCheckpoints(tunnelId: String, uuid: String): Map[TunnelChannel, ChannelOffset] = {
    val rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName)
    val beginPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    beginPk.addPrimaryKeyColumn(TUNNEL_ID_COLUMN, PrimaryKeyValue.fromString(tunnelId))
    beginPk.addPrimaryKeyColumn(UUID_COLUMN, PrimaryKeyValue.fromString(uuid))
    beginPk.addPrimaryKeyColumn(CHANNEL_ID_COLUMN, PrimaryKeyValue.INF_MIN)
    rangeRowQueryCriteria.setInclusiveStartPrimaryKey(beginPk.build())

    val endPk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    endPk.addPrimaryKeyColumn(TUNNEL_ID_COLUMN, PrimaryKeyValue.fromString(tunnelId))
    endPk.addPrimaryKeyColumn(UUID_COLUMN, PrimaryKeyValue.fromString(uuid))
    endPk.addPrimaryKeyColumn(CHANNEL_ID_COLUMN, PrimaryKeyValue.INF_MAX)
    rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPk.build())
    rangeRowQueryCriteria.setMaxVersions(1)

    val retMap = mutable.Map[TunnelChannel, ChannelOffset]()
    var isFinished = false
    while (!isFinished) {
      val getRangeResponse = syncClient.getRange(new GetRangeRequest(rangeRowQueryCriteria))
      getRangeResponse.getRows.asScala.foreach { row =>
        val channelId = row.getPrimaryKey.getPrimaryKeyColumn(CHANNEL_ID_COLUMN).getValue.asString
        val channelOffset =
          ChannelOffset.deserialize(row.getLatestColumn(CHANNEL_OFFSET_COLUMN).getValue.asString)
        retMap.put(TunnelChannel(tunnelId, channelId), channelOffset)
      }

      if (getRangeResponse.getNextStartPrimaryKey != null) {
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey)
      } else {
        isFinished = true
      }
    }
    logInfo(s"get meta checkpoints: ${retMap}")
    retMap.toMap
  }
}

private object MetaCheckpointer {
  val TUNNEL_ID_COLUMN = "tunnelId"
  val UUID_COLUMN = "uuid"
  val CHANNEL_ID_COLUMN = "channelId"
  val CHANNEL_OFFSET_COLUMN = "channelOffset"
  val VERSION_COLUMN = "version"
  val CURRENT_VERSION = 1
}
