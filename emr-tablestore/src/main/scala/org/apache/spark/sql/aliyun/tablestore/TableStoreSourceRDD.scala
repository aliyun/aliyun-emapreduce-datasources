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

import java.util.concurrent.LinkedBlockingQueue

import com.alicloud.openservices.tablestore.model.tunnel.internal.{ReadRecordsRequest, ReadRecordsResponse}
import com.alicloud.openservices.tablestore.model.{ColumnType, PrimaryKeyType, RecordColumn, StreamRecord}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aliyun.tablestore.TableStoreSourceProvider._
import org.apache.spark.sql.types._
import org.apache.spark.util.NextIterator
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class TableStoreSourceRDDOffsetRange(
    tunnelChannel: TunnelChannel,
    startOffset: ChannelOffset,
    endOffset: ChannelOffset) {
  def table: String = tunnelChannel.tableName
  def tunnel: String = tunnelChannel.tunnelId
  def channel: String = tunnelChannel.channelId
}

case class TableStoreSourceRDDPartition(index: Int, offsetRange: TableStoreSourceRDDOffsetRange) extends Partition

class TableStoreSourceRDD(
    sc: SparkContext,
    instanceName: String,
    tableName: String,
    tunnelId: String,
    accessKeyId: String,
    accessKeySecret: String,
    endpoint: String,
    channelOffsets: ArrayBuffer[(String, ChannelOffset, ChannelOffset)],
    schema: StructType,
    maxOffsetsPerChannel: Long = -1L,
    checkpointTable: String) extends RDD[TableStoreData](sc, Nil) with Logging {
  // check whether channel has enough new data.
  def channelIsEmpty(cp: ChannelPartition): Boolean = {
    if (cp.startOffset.logPoint == OTS_CHANNEL_FINISHED) {
      return false
    }
    val tunnelClient = TableStoreOffsetReader.getOrCreateTunnelClient(
      endpoint, accessKeyId, accessKeySecret, instanceName)
    tunnelClient.readRecords(
      new ReadRecordsRequest(tunnelId, TUNNEL_CLIENT_TAG, cp.channelId, cp.startOffset.logPoint)
    ).getRecords.isEmpty
  }

  override def compute(split: Partition, context: TaskContext): Iterator[TableStoreData] = {
    val channelPartition = split.asInstanceOf[ChannelPartition]
    val checkpointer = new TableStoreCheckpointer(
      TableStoreOffsetReader.getOrCreateSyncClient(endpoint, accessKeyId, accessKeySecret, instanceName),
      checkpointTable
    )

    // TODO: Seeking a better solution for below logic
    if (channelIsEmpty(channelPartition)) {
      logInfo(s"channel ${channelPartition} hasn't new records")
      if (channelPartition.startOffset != ChannelOffset.TERMINATED_CHANNEL_OFFSET) {
        checkpointer.checkpoint(
          TunnelChannel(tableName, tunnelId, channelPartition.channelId),
          channelPartition.startOffset
        )
      }
      // here, add some backoff timeout, forbid pull data crazy.
      Thread.sleep(100 + new Random(System.currentTimeMillis()).nextInt(1000))
      return Iterator.empty.asInstanceOf[Iterator[TableStoreData]]
    }

    val schemaFieldPos: Map[String, Int] = schema.fieldNames
      .filter(fieldName => !isDefaultField(fieldName))
      .zipWithIndex
      .toMap
    val schemaFieldPosSize = schemaFieldPos.size // one for column, one for columnType

    try {
      new InterruptibleIterator[TableStoreData](
        context,
        new NextIterator[TableStoreData] {
          private val totalCount = channelPartition.count
          private var hasRead: Int = 0
          private var nextOffset: ChannelOffset = channelPartition.startOffset
          private var logData = new LinkedBlockingQueue[TableStoreData](10000)
          private val channelId = channelPartition.channelId
          private var isFirstFetch = true
          private val inputMetrics = context.taskMetrics().inputMetrics
          private val tunnelClient =
            TableStoreOffsetReader.getOrCreateTunnelClient(endpoint, accessKeyId, accessKeySecret, instanceName)

          context.addTaskCompletionListener { _ => closeIfNeeded() }

          logInfo(s"In rdd compute, ${endpoint}, ${instanceName}, ${tableName}, ${tunnelId}, ${channelId}")

          def checkHasNext(): Boolean = {
            if (totalCount < 0) {
              logData.nonEmpty
            } else {
              val hasNext = hasRead <= totalCount && logData.nonEmpty
              hasNext
            }
          }

          def fetchNextBatch(): Unit = {
            if (nextOffset == ChannelOffset.TERMINATED_CHANNEL_OFFSET) {
              return
            }
            val recordsResp: ReadRecordsResponse = tunnelClient.readRecords(
              new ReadRecordsRequest(tunnelId, TUNNEL_CLIENT_TAG, channelId, nextOffset.logPoint)
            )
            if (totalCount >= 0 && hasRead + recordsResp.getRecords.size > totalCount) {
              logInfo(s"Exceed the count limit, go to next batch, hasRead: ${hasRead}")
              return
            }
            var count = 0
            recordsResp.getRecords.foreach(record => {
              val recordType = record.getRecordType.name
              val recordTimeStamp = record.getSequenceInfo.getTimestamp
              val columnArray = Array.tabulate(schemaFieldPosSize)(
                _ => (null, null).asInstanceOf[(String, Any)]
              )
              schemaFieldPos.foreach { case (fieldName, idx) =>
                var colVal: Option[Any] = None
                if (fieldName.contains(__OTS_COLUMN_TYPE_PREFIX)) {
                  colVal = extractColumnType(record, fieldName.stripPrefix(__OTS_COLUMN_TYPE_PREFIX))
                } else {
                  colVal = extractValue(record, fieldName)
                }
                if (colVal.isDefined) {
                  columnArray(idx) = (fieldName, colVal.get)
                }
              }
              count += 1
              logData.offer(new SchemaTableStoreData(recordType, recordTimeStamp, columnArray))
            })

            val crt = nextOffset
            if (recordsResp.getNextToken == null) {
              nextOffset = ChannelOffset.TERMINATED_CHANNEL_OFFSET
            } else {
              nextOffset = ChannelOffset(recordsResp.getNextToken, 0L)
            }
            logInfo(
              s"channelId: ${channelPartition.channelId}, currentOffset: $crt, nextOffset: $nextOffset, " +
                s"hasRead: $hasRead, get: $count, queue: ${logData.size}, totalCount: ${totalCount}"
            )
          }

          override protected def getNext(): TableStoreData = {
            if (isFirstFetch) {
              logInfo("do first fetch")
              fetchNextBatch()
              logInfo("finish fetch first batch")
              isFirstFetch = false
            }
            finished = !checkHasNext()
            if (!finished) {
              if (logData.size() == 1) {
                logInfo("fetch next batch")
                fetchNextBatch()
              }
              hasRead += 1
              logData.poll()
            } else {
              logInfo(s"Current logData, hasRead: ${hasRead}, logData: ${logData.size}")
              checkpointer.checkpoint(
                TunnelChannel(tableName, tunnelId, channelId),
                nextOffset
              )
              logInfo(s"set endOffset, channelId: ${channelId}, nextOffset: ${nextOffset}")
              null.asInstanceOf[TableStoreData]
            }
          }

          override protected def close(): Unit = {
            try {
              inputMetrics.incBytesRead(hasRead)
              logData.clear()
              logData = null
            } catch {
              case e: Exception => logWarning("Got exception when close TableStore channel iterator.", e)
            }
          }

        }
      )
    } catch {
      case _: Exception => Iterator.empty.asInstanceOf[Iterator[TableStoreData]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    channelOffsets.zipWithIndex.map { case (p, idx) =>
      new ChannelPartition(
        id,
        idx,
        p._1,
        instanceName,
        tableName,
        tunnelId,
        accessKeyId,
        accessKeySecret,
        endpoint,
        p._2,
        p._3,
        maxOffsetsPerChannel
      ).asInstanceOf[Partition]
    }.toArray
  }

  private class ChannelPartition(
      rddId: Int,
      partitionId: Int,
      val channelId: String,
      instanceName: String,
      tableName: String,
      val tunnelId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      val startOffset: ChannelOffset,
      val endOffset: ChannelOffset,
      val count: Long = -1L) extends Partition with Logging {
    override def hashCode(): Int = 41 * (41 + rddId) + channelId.hashCode

    override def index: Int = partitionId
  }

  // Cause TableStore is free schema, filter the mismatch columns.
  private def extractValue(record: StreamRecord, fieldName: String): Option[Any] = {
    TableStoreSourceRDD.extractValue(record, fieldName, schema)
  }

  private def extractColumnType(record: StreamRecord, fieldName: String): Option[Any] = {
    TableStoreSourceRDD.extractColumnType(record, fieldName)
  }
}

object TableStoreSourceRDD extends Logging {
  private[sql] def extractValue(
      record: StreamRecord,
      fieldName: String,
      schema: StructType): Option[Any] = {
    val isPrimaryKey = record.getPrimaryKey.contains(fieldName)
    val attributeColumnsMap = getAttributeColumnsMap(record)
    val isAttributeKey = attributeColumnsMap.keySet.contains(fieldName)
    if (isPrimaryKey) {
      val pkColumn = record.getPrimaryKey.getPrimaryKeyColumn(fieldName)
      pkColumn.getValue.getType match {
        case PrimaryKeyType.INTEGER =>
          schema(pkColumn.getName).dataType match {
            case LongType => Some(pkColumn.getValue.asLong())
            case IntegerType => Some(pkColumn.getValue.asLong().toInt)
            case FloatType => Some(pkColumn.getValue.asLong().toFloat)
            case DoubleType => Some(pkColumn.getValue.asLong().toDouble)
            case ShortType => Some(pkColumn.getValue.asLong().toShort)
            case ByteType => Some(pkColumn.getValue.asLong().toByte)
            case _ =>
              throw new IllegalArgumentException(
                s"data type mismatch, expected: ${schema(pkColumn.getName).dataType} real: ${pkColumn.getValue.getType}"
              )
          }
        case PrimaryKeyType.STRING => Some(pkColumn.getValue.asString())
        case PrimaryKeyType.BINARY => Some(pkColumn.getValue.asBinary())
        case _ =>
          throw new IllegalArgumentException(
            s"unknown data type of primary key: ${pkColumn.getValue.getType}"
          )
      }
    } else if (isAttributeKey) {
      val col = attributeColumnsMap(fieldName).getColumn
      val schemaType = schema(col.getName).dataType
      val columnType = col.getValue.getType
      if (!checkTypeMatched(schemaType, columnType)) {
        logWarning(s"column [${col.getName}] data type mismatch, expected: ${schemaType} real: ${columnType}")
        return None
      }
      columnType match {
        case ColumnType.INTEGER =>
          val value = col.getValue.asLong()
          schemaType match {
            case LongType => Some(value.toLong)
            case IntegerType => Some(value.toInt)
            case FloatType => Some(value.toFloat)
            case DoubleType => Some(value.toDouble)
            case ShortType => Some(value.toShort)
            case ByteType => Some(value.toByte)
            case _ =>
              None
          }
        case ColumnType.DOUBLE => Some(col.getValue.asDouble())
        case ColumnType.STRING => Some(col.getValue.asString())
        case ColumnType.BOOLEAN => Some(col.getValue.asBoolean())
        case ColumnType.BINARY => Some(col.getValue.asBinary())
        case _ => None
      }
    } else {
      None
    }
  }

  private def checkTypeMatched(schemaType: DataType, columnType: ColumnType): Boolean = {
    columnType match {
      case ColumnType.INTEGER =>
        schemaType match {
          case LongType | IntegerType | FloatType | DoubleType | ShortType | ByteType => true
          case _ => false
        }
      case ColumnType.DOUBLE =>
        schemaType match {
          case DoubleType => true
          case _ => false
        }
      case ColumnType.STRING =>
        schemaType match {
          case StringType => true
          case _ => false
        }
      case ColumnType.BOOLEAN =>
        schemaType match {
          case BooleanType => true
          case _ => false
        }
      case ColumnType.BINARY =>
        schemaType match {
          case BinaryType => true
          case _ => false
        }
      case _ => false
    }
  }

  private[sql] def extractColumnType(record: StreamRecord,
                                     fieldName: String): Option[Any] = {
    val attributeColumnsMap = getAttributeColumnsMap(record)
    if (attributeColumnsMap.contains(fieldName)) {
      Some(attributeColumnsMap(fieldName).getColumnType.name)
    } else {
      None
    }
  }

  private def getAttributeColumnsMap(record: StreamRecord): Map[String, RecordColumn] = {
    record.getColumns.map(column => (column.getColumn.getName, column)).toMap
  }
}
