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

package org.apache.spark.aliyun.odps.reader

import java.io.EOFException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.aliyun.odps.{Column, PartitionSpec}
import org.apache.commons.lang.StringUtils

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.odps.OdpsPartition
import org.apache.spark.aliyun.odps.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NextIterator, TaskCompletionListener}

/**
 * Single Partition Reader
 * @param split
 * @param context
 * @param requiredSchema
 */
private[spark] class ODPSTableIterator(
    maxInFlight: Int,
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

  private val odpsUtils = OdpsUtils(split)
  private val tunnel = odpsUtils.getTableTunnel

  private val isPartitionTable: Boolean = odpsUtils.isPartitionTable(split.project, split.table)
  private val tableSchema = odpsUtils.getTableSchema(split.project, split.table)
  private val partition = Option(split.part).map(p => new PartitionSpec(p)).orNull

  validatePartition()

  private var lastReadableRows: Long = split.count
  private val inputMetrics = context.taskMetrics.inputMetrics

  case class RowWrapper(row: Option[InternalRow] = None, e: Option[Throwable] = None) {
    def get(): InternalRow = if (row.isDefined) {
      inputMetrics.incRecordsRead(1L)
      row.get
    } else if (e.isDefined) {
      throw e.get
    } else {
      null
    }
  }

  private val reader = {
    val requiredColumns = requiredSchema.fields.collect {
      case f if tableSchema.containsColumn(f.name) =>
        new Column(f.name, OdpsUtils.sparkTypeToOdpsType(f.dataType), f.getComment().orNull)
    }.toList.asJava
    val requiredPartitionColumns = requiredSchema.fields.collect {
      case f if tableSchema.containsPartitionColumn(f.name) =>
        new Column(f.name, OdpsUtils.sparkTypeToOdpsType(f.dataType), f.getComment().orNull)
    }.toList.asJava

    val session = if (!isPartitionTable) {
      tunnel.createDownloadSession(split.project, split.table)
    } else {
      tunnel.createDownloadSession(split.project, split.table, partition)
    }

    if (requiredColumns.isEmpty && requiredPartitionColumns.isEmpty) {
      session.openRecordReader(split.start, split.count, true)
    } else if (!requiredColumns.isEmpty) {
      session.openRecordReader(split.start, split.count, true, requiredColumns)
    } else {
      logInfo(s"Table ${split.project}.${split.table} non-partitioned column schema is " +
        s"${tableSchema.getColumns.asScala.map(_.getName).mkString(",")}, but require" +
        s" ${requiredSchema.fieldNames.mkString(",")}.")
      null
    }
  }
  private val readFinished = new AtomicBoolean(false)
  private val queue = new LinkedBlockingQueue[RowWrapper](maxInFlight)

  start()

  override def getNext(): InternalRow = {
    try {
      while (!readFinished.get() || queue.size() > 0) {
        val result = queue.poll(100, TimeUnit.MILLISECONDS)
        if (result != null) {
          return result.get()
        }
      }
      finished = true
      null
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
    if (isPartitionTable && (split.part == null || split.part.isEmpty || partition == null)) {
      val errorMessage = s"Table ${split.project}.${split.table}" +
        s" is a partition table but without partitionSpec."
      throw new IllegalArgumentException(errorMessage)
    }

    if (!isPartitionTable && (split.part != null && split.part.nonEmpty || partition != null)) {
      val errorMessage = s"Table ${split.project}.${split.table} is not a partition table " +
        s"but specify partitionSpec ${split.part}"
      throw new IllegalArgumentException(errorMessage)
    }

    tableSchema.getPartitionColumns.asScala.foreach { pColumn =>
      if (partition == null || StringUtils.isBlank(partition.get(pColumn.getName))) {
        val errorMessage = s"Table ${split.project}.${split.table}" +
          s" didn't contain partition ${pColumn.getName}"
        throw new IllegalArgumentException(errorMessage)
      }
    }
  }

  private def start(): Unit = {
    val dataTypes = requiredSchema.fields.map(_.dataType)
    new Thread(new Runnable {
      override def run(): Unit = {
        try {
          while (lastReadableRows > 0) {
            val mutableRow = new SpecificInternalRow(dataTypes)

            if (reader == null) {
              if (partition == null) {
                throw new IllegalStateException(
                  "While reader is null, partition spec couldn't be null.")
              }

              requiredSchema.zipWithIndex.foreach {
                case (field, idx) =>
                  val value = partition.get(field.name)
                  val dataType = OdpsUtils.sparkTypeToOdpsType(field.dataType)
                  mutableRow.update(idx, OdpsUtils.odpsData2SparkData(dataType)(value))
              }
            } else {
              val record = reader.read()

              requiredSchema.zipWithIndex.foreach {
                case (field, idx) =>
                  val (column, value) = if (tableSchema.containsPartitionColumn(field.name)) {
                    (tableSchema.getPartitionColumn(field.name), partition.get(field.name))
                  } else {
                    (tableSchema.getColumn(field.name), record.get(field.name))
                  }
                  mutableRow.update(idx, OdpsUtils.odpsData2SparkData(column.getTypeInfo)(value))
              }
            }

            queue.put(RowWrapper(Some(mutableRow)))
            lastReadableRows -= 1
          }
        } catch {
          case e: Exception =>
            queue.clear()
            queue.put(RowWrapper(None, Some(e)))

            logError("Can not transfer record column value.", e)
            throw e
        } finally {
          readFinished.compareAndSet(false, true)
        }
      }
    }, s"ODPS-Record-Reader-${TaskContext.get().taskAttemptId()}").start()
  }

}
