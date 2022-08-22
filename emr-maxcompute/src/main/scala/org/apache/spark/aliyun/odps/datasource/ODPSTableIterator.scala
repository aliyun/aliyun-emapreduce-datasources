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

package org.apache.spark.aliyun.odps.datasource

import java.io.EOFException

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

import com.aliyun.odps.{PartitionSpec, TableSchema}
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelRecordReader

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.odps.OdpsPartition
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.{NextIterator, TaskCompletionListener}

/**
 * Single Partition Reader
 * @param split
 * @param context
 * @param requiredSchema
 */
private[spark] class ODPSTableIterator(
    split: OdpsPartition,
    context: TaskContext,
    requiredSchema: StructType)
  extends NextIterator[InternalRow]
  with Logging {

  context.addTaskCompletionListener(new TaskCompletionListener {
    override def onTaskCompletion(context: TaskContext): Unit = {
      closeIfNeeded()
    }
  })

  private val odpsUtils = OdpsUtils(split.accessKeyId, split.accessKeySecret, split.odpsUrl)
  private val tunnel = odpsUtils.getTableTunnel(split.tunnelUrl)

  private val isPartitionTable: Boolean = odpsUtils.isPartitionTable(split.table, split.project)
  private val tableSchema: TableSchema = odpsUtils.getTableSchema(split.project, split.table)
  private val partition: Map[String, String] = Option(split.part)
    .map(_.split("/").map(spec => {
      val nameAndValue = spec.split("=")
      if (nameAndValue.length != 2) {
        throw new IllegalArgumentException("PartitionSpec must be specified as 'a=b'")
      }
      (nameAndValue(0), nameAndValue(1))
    }).toMap).getOrElse(new HashMap[String, String])

  validatePartition()

  private val session: TableTunnel#DownloadSession = if (!isPartitionTable) {
    tunnel.createDownloadSession(split.project, split.table)
  } else {
    val partSpec = new PartitionSpec(split.part)
    tunnel.createDownloadSession(split.project, split.table, partSpec)
  }
  private val reader: TunnelRecordReader = session.openRecordReader(split.start, split.count)
  private var lastReadableRows: Long = split.count

  private val mutableRow = new SpecificInternalRow(requiredSchema.fields.map(x => x.dataType))
  private val inputMetrics = context.taskMetrics.inputMetrics

  override def getNext(): InternalRow = {
    try {
      if (lastReadableRows <= 0) {
        finished = true
        return null.asInstanceOf[InternalRow]
      }
      val record = reader.read()
      lastReadableRows -= 1

      requiredSchema.zipWithIndex.foreach {
        case (s: StructField, idx: Int) =>
          try {
            val (typeInfo, value) = if (isPartitionTable && partition.contains(s.name)) {
              (tableSchema.getPartitionColumn(s.name).getTypeInfo, partition(s.name))
            } else {
              (tableSchema.getColumn(s.name).getTypeInfo, record.get(s.name))
            }
            mutableRow.update(idx, OdpsUtils.odpsData2SparkData(typeInfo)(value))
          } catch {
            case e: Exception =>
              log.error(s"Can not transfer record column value, idx: $idx, type: ${s.dataType}")
              throw e
          }
      }

      inputMetrics.incRecordsRead(1L)
      mutableRow
    } catch {
      case _: EOFException =>
        finished = true
        null.asInstanceOf[InternalRow]
    }
  }

  override def close(): Unit = {
    try {
      if (reader != null) {
        inputMetrics.incBytesRead(reader.getTotalBytes)
        reader.close()
      }
    } catch {
      case e: Exception => logWarning("Exception in RecordReader.close()", e)
    }
  }

  private def validatePartition(): Unit = {
    if (isPartitionTable && (split.part == null || split.part.isEmpty || partition.isEmpty)) {
      val errorMessage = s"Table ${split.project}.${split.table}" +
        s" is a partition table but without partitionSpec."
      throw new IllegalArgumentException(errorMessage)
    }

    if (!isPartitionTable && (split.part != null && split.part.nonEmpty || partition.nonEmpty)) {
      val errorMessage = s"Table ${split.project}.${split.table} is not a partition table " +
        s"but specify partitionSpec ${split.part}"
      throw new IllegalArgumentException(errorMessage)
    }

    val partitionCols = tableSchema.getPartitionColumns.asScala.map(_.getName.toLowerCase()).toSet
    partition.foreach(part => {
      if (!partitionCols.contains(part._1.toLowerCase())) {
        val errorMessage = s"Table ${split.project}.${split.table}" +
          s" didn't contain partition ${part._1}"
        throw new IllegalArgumentException(errorMessage)
      }
    })
  }
}
