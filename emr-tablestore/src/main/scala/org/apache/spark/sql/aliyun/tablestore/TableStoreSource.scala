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

import java.util.UUID

import com.alicloud.openservices.tablestore.model._
import com.alicloud.openservices.tablestore.model.tunnel.internal.{CheckpointRequest, GetCheckpointRequest}
import com.alicloud.openservices.tablestore.{SyncClientInterface, TunnelClientInterface}
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.tablestore.TableStoreSourceProvider._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.aliyun.tablestore.MetaCheckpointer._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class TableStoreSource(
    @transient sqlContext: SQLContext,
    userSpecifiedSchema: Option[StructType],
    @transient tableStoreOffsetReader: TableStoreOffsetReader,
    sourceOptions: Map[String, String],
    metadataPath: String)
  extends Source with Logging with Serializable {

  private val accessKeyId = sourceOptions.getOrElse(
    "access.key.id",
    throw new MissingArgumentException("Missing access key id (='access.key.id').")
  )

  private val accessKeySecret = sourceOptions.getOrElse(
    "access.key.secret",
    throw new MissingArgumentException("Missing access key secret (='access.key.secret').")
  )

  private val endpoint = sourceOptions.getOrElse(
    "endpoint",
    throw new MissingArgumentException("Missing log store endpoint (='endpoint').")
  )

  private val instanceName = sourceOptions.getOrElse(
    "instance.name",
    throw new MissingArgumentException("Missing TableStore instance (='instance.name').")
  )

  private val tableName = sourceOptions.getOrElse(
    "table.name",
    throw new MissingArgumentException("Missing TableStore table (='table.name').")
  )

  private val tunnelId = sourceOptions.getOrElse(
    "tunnel.id",
    throw new MissingArgumentException("Missing TableStore tunnel (='tunnel.id').")
  )

  private val checkpointTable = sourceOptions.getOrElse("checkpoint.table", "__spark_meta_checkpoint__")

  @transient var tunnelClient: TunnelClientInterface =
    TableStoreOffsetReader.getOrCreateTunnelClient(endpoint, accessKeyId, accessKeySecret, instanceName)

  @transient var syncClient: SyncClientInterface =
    TableStoreOffsetReader.getOrCreateSyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)

  private[sql] lazy val initialOffsetUUID = {
    logInfo("initialOffsetUUID")
    initialMetaCheckpointer
    val metadataLog = new TableStoreInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val channelOffsets = tableStoreOffsetReader.fetchOffsetsFromTunnel()
      val offset = TableStoreSourceOffset(UUID.randomUUID().toString)
      metaCheckpointer.checkpoint(offset.uuid, channelOffsets)
      metadataLog.add(0, offset)
      logInfo(s"Initial offsets: $offset")
      offset
    }.uuid
  }

  private[sql] lazy val initialMetaCheckpointer = {
    logInfo("initialMetaCheckpointer")
    // create meta checkpoint table if not exist.
    if (!syncClient.listTable().getTableNames.contains(checkpointTable)) {
      val tableMeta: TableMeta = new TableMeta(checkpointTable)
      tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TUNNEL_ID_COLUMN, PrimaryKeyType.STRING))
      tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(UUID_COLUMN, PrimaryKeyType.STRING))
      tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(CHANNEL_ID_COLUMN, PrimaryKeyType.STRING))
      // default ttl: 7 days, user can custom this value by Console or SDK.
      val tableOptions: TableOptions = new TableOptions(7 * 86400, 1)
      try {
        syncClient.createTable(new CreateTableRequest(tableMeta, tableOptions))
      } catch {
        case NonFatal(ex) => logInfo(s"Non fatal exception! $ex")
      }
    }
  }

  private[sql] val innerSchema = TableStoreSource.tableStoreSchema(
    userSpecifiedSchema.getOrElse(TableStoreCatalog(sourceOptions).schema)
  )

  override def schema: StructType = innerSchema

  private val maxOffsetsPerChannel =
    sourceOptions.getOrElse(TableStoreSourceProvider.MAX_OFFSETS_PER_CHANNEL, 10000 + "")

  private val metaCheckpointer = new MetaCheckpointer(syncClient, checkpointTable)

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    initialOffsetUUID

    val uuid = UUID.randomUUID().toString
    logInfo(s"get offset: ${uuid}")
    Some(TableStoreSourceOffset(uuid))
  }

  // Used for testSuite.
  private[sql] var currentBatchRDD = sqlContext.sparkContext.emptyRDD[InternalRow]

  /**
   * Returns the data that is between the offsets
   * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
   * exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startUUID = start match {
      case Some(prevBatchEndOffset) => TableStoreSourceOffset.getUUID(prevBatchEndOffset)
      case None => initialOffsetUUID
    }
    val endUUID = TableStoreSourceOffset.getUUID(end)
    logInfo(s"current start: ${startUUID}, end: ${endUUID}")

    // Get real start ChannelOffsets.
    val channelOffsets = new ArrayBuffer[(String, ChannelOffset, ChannelOffset)]
    val startChannelOffsets = metaCheckpointer.getCheckpoints(tunnelId, startUUID)
    val tunnelOffsets = tableStoreOffsetReader.fetchOffsetsFromTunnel()
    // use below logic to check whether have new channels.
    (tunnelOffsets ++ startChannelOffsets).foreach(po => {
      channelOffsets.+=((po._1.channelId, po._2, null))
    })
    logInfo(s"current channelOffsets: ${channelOffsets}")

    val encoderForDataColumns = RowEncoder(schema).resolveAndBind()
    val rdd = new TableStoreSourceRDD(
      sqlContext.sparkContext,
      instanceName,
      tableName,
      tunnelId,
      accessKeyId,
      accessKeySecret,
      endpoint,
      channelOffsets,
      schema,
      maxOffsetsPerChannel.toLong,
      checkpointTable,
      TableStoreSourceOffset.getUUID(end)
    ).map(it => {
      encoderForDataColumns.toRow(new GenericRow(it.toArray))
    })

    currentBatchRDD = rdd
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = {
    // commit the prev batch end offset to Tablestore tunnel
    val uuid = TableStoreSourceOffset.getUUID(end)
    val channelOffsets = metaCheckpointer.getCheckpoints(tunnelId, uuid)
    channelOffsets.foreach{ case (tc, co) =>
      logInfo(s"Checkpoint to TableStore, channel: ${tc}, channelOffset: ${co}")
      tunnelClient.checkpoint(
        new CheckpointRequest(tc.tunnelId, TUNNEL_CLIENT_TAG, tc.channelId, co.logPoint, 0)
      )
      // when channel is finished, remove it from meta table.
      if (co == ChannelOffset.TERMINATED_CHANNEL_OFFSET) {
        metaCheckpointer.deleteCheckpoint(tc, uuid)
      }
    }
  }

  override def stop(): Unit = {
    if (syncClient != null) {
      syncClient.shutdown()
    }
    if (tunnelClient != null) {
      tunnelClient.shutdown()
    }
    tableStoreOffsetReader.close()
  }
}

object TableStoreSource extends Logging {
  def tableStoreSchema(userStructType: StructType): StructType = {
    var schema = new StructType()
    schema = schema.add(StructField(__OTS_RECORD_TYPE__, StringType))
    schema = schema.add(StructField(__OTS_RECORD_TIMESTAMP__, LongType))

    userStructType.fields.foreach { field =>
      schema = schema.add(field)
      if (!isDefaultField(field.name)) {
        schema = schema.add(StructField(__OTS_COLUMN_TYPE_PREFIX + field.name, StringType))
      }
    }
    logInfo(s"TableStore Schema: ${schema}")
    schema
  }
}
