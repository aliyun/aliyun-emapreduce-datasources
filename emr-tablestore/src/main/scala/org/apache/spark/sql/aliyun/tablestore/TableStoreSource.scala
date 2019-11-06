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

import com.alicloud.openservices.tablestore.model._
import com.alicloud.openservices.tablestore.model.tunnel.internal.{CheckpointRequest, GetCheckpointRequest}
import com.alicloud.openservices.tablestore.{SyncClientInterface, TunnelClientInterface}
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.tablestore.TableStoreSourceProvider._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

class TableStoreSource(
    @transient sqlContext: SQLContext,
    userSpecifiedSchema: Option[StructType],
    @transient tableStoreOffsetReader: TableStoreOffsetReader,
    sourceOptions: Map[String, String],
    metadataPath: String)
  extends Source with Logging with Serializable {

  // private sql scope for testsuite.
  private[sql] val batches = new mutable.HashMap[(Option[Offset], Offset), TableStoreSourceRDD]()

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

  private val checkpointTable = sourceOptions.getOrElse("ots.checkpoint.table", "__spark_checkpoint__")

  @transient var tunnelClient: TunnelClientInterface =
    TableStoreOffsetReader.getOrCreateTunnelClient(endpoint, accessKeyId, accessKeySecret, instanceName)

  @transient var syncClient: SyncClientInterface =
    TableStoreOffsetReader.getOrCreateSyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)

  private[sql] lazy val initialPartitionOffsets = {
    logInfo("initialPartitionOffsets")
    val metadataLog = new TableStoreInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = TableStoreSourceOffset(tableStoreOffsetReader.fetchStartOffsets())
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.channelOffsets
  }

  private[sql] lazy val initialCheckpointer = {
    logInfo("initialCheckpointer")
    // create meta table if not exist.
    val tableMeta: TableMeta = new TableMeta(checkpointTable)
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("TunnelId", PrimaryKeyType.STRING))
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("ChannelId", PrimaryKeyType.STRING))
    val tableOptions: TableOptions = new TableOptions(-1, 1)
    try {
      syncClient.createTable(new CreateTableRequest(tableMeta, tableOptions))
    } catch {
      case NonFatal(ex) => logInfo(s"Non fatal exception! $ex")
    }
  }

  private var isInitial = true

  private[sql] val innerSchema = TableStoreSource.tableStoreSchema(
    userSpecifiedSchema.getOrElse(TableStoreCatalog(sourceOptions).schema)
  )

  override def schema: StructType = innerSchema

  private val maxOffsetsPerChannel =
    sourceOptions.getOrElse(TableStoreSourceProvider.MAX_OFFSETS_PER_CHANNEL, 10000 + "")

  private val checkpointer = new TableStoreCheckpointer(syncClient, checkpointTable)

  def fetchMergedStartOffsets(): Map[TunnelChannel, ChannelOffset] = {
    tableStoreOffsetReader.runUninterruptibly {
      val committedOffsets = tableStoreOffsetReader.fetchOffsetsFromTunnel()
      val currentStartOffsets = checkpointer.getTunnelCheckpoints(tableName, tunnelId)
      logInfo(s"CommittedOffsets: ${committedOffsets}, CurrentStartOffsets: ${currentStartOffsets}")

      // handle Terminated channel.
      committedOffsets ++ currentStartOffsets.map { case (tc, co) =>
        if (co == ChannelOffset.TERMINATED_CHANNEL_OFFSET) {
          logInfo(s"Checkpoint to TableStore, channel: ${tc}, channelOffset: ${co}")
          tunnelClient.checkpoint(
            new CheckpointRequest(tc.tunnelId, TUNNEL_CLIENT_TAG, tc.channelId, co.logPoint, 0)
          )
          checkpointer.deleteCheckpoint(tc)
        }
        (tc, co)
      }.filter(offset => offset._2 != ChannelOffset.TERMINATED_CHANNEL_OFFSET)
    }
  }

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    logInfo(s"begin get offset")
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets
    // Initial Tablestore checkpointer
    initialCheckpointer

    val startOffsets = if (isInitial) {
      isInitial = false
      tableStoreOffsetReader.fetchOffsetsFromTunnel()
    } else {
      fetchMergedStartOffsets()
    }

    logInfo(s"Current startOffsets: ${startOffsets}")

    // ChannelId, BeginOffset, EndOffset
    val channelOffsets = new ArrayBuffer[(String, ChannelOffset, ChannelOffset)]
    startOffsets.foreach(po => {
      // Cause TableStore not support fetch latest offset,
      // the endOffset would be filled after RDD's compute logic via [[TableStoreCheckpointer]]
      channelOffsets.+=((po._1.channelId, po._2, null))
    })

    batches.foreach(e => e._2.unpersist(false))
    batches.clear()

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
      checkpointTable
    )
    sqlContext.sparkContext.setLocalProperty(EXECUTION_ID_KEY, null)
    rdd.persist(StorageLevel.MEMORY_AND_DISK).count()

    val start = TableStoreSourceOffset(channelOffsets.map(co => (tableName, tunnelId, co._1, co._2)).toArray: _*)

    val end = TableStoreSourceOffset(channelOffsets.map(co =>
      (tableName, tunnelId, co._1, checkpointer.getCheckpoint(TunnelChannel(tableName, tunnelId, co._1)))).toArray: _*
    )

    batches.put((Some(start), end), rdd)
    logInfo(s"end get offset, ${end}")
    Some(end)
  }

  // Used for testSuite.
  private[sql] var currentBatchRDD = sqlContext.sparkContext.emptyRDD[InternalRow]

  /**
   * Returns the data that is between the offsets
   * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
   * exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val initialStart = if (start.isEmpty) {
      Some(TableStoreSourceOffset(initialPartitionOffsets))
    } else {
      start
    }

    val rdd = if (batches.contains((initialStart, end))) {
      val expiredBatches = batches.filter(
        b => !b._1._1.equals(initialStart) || !b._1._2.equals(end)
      )
      expiredBatches.foreach(_._2.unpersist())
      expiredBatches.foreach(b => batches.remove(b._1))
      val encoderForDataColumns = RowEncoder(schema).resolveAndBind()
      batches((initialStart, end)).map(it => {
        encoderForDataColumns.toRow(new GenericRow(it.toArray))
      })
    } else {
      logInfo(s"Batches not contains RDD")
      val fromChannelOffsets = start match {
        case Some(prevBatchEndOffset) => TableStoreSourceOffset.getChannelOffsets(prevBatchEndOffset)
        case None => initialPartitionOffsets
      }
      val channelOffsets = new ArrayBuffer[(String, ChannelOffset, ChannelOffset)]
      val untilChannelOffsets = TableStoreSourceOffset.getChannelOffsets(end)
      val channels = untilChannelOffsets.keySet.filter {
        // Ignore channels that we don't know the from offsets.
        channel => fromChannelOffsets.contains(channel)
      }.toSeq
      channels.foreach(channel => {
        channelOffsets.+=((channel.channelId, fromChannelOffsets(channel), untilChannelOffsets(channel)))
      })

      val encoderForDataColumns = RowEncoder(schema).resolveAndBind()
      new TableStoreSourceRDD(
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
        checkpointTable
      ).map(it => {
        encoderForDataColumns.toRow(new GenericRow(it.toArray))
      })
    }
    currentBatchRDD = rdd
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  def checkpointToTunnel(offset: Offset): Unit = {
    val sourceOffset = TableStoreSourceOffset.getChannelOffsets(offset)
    sourceOffset.foreach { case (tc, offset) =>
      logInfo(s"Checkpoint to TableStore, channel: ${tc}, channelOffset: ${offset}")
      tunnelClient.checkpoint(
        new CheckpointRequest(tc.tunnelId, TUNNEL_CLIENT_TAG, tc.channelId, offset.logPoint, 0)
      )
    }
  }

  override def commit(end: Offset): Unit = {
    // commit the prev batch endoffset to Tablestore tunnel
    checkpointToTunnel(end)
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
