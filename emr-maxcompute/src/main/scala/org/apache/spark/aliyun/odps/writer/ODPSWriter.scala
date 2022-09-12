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
package org.apache.spark.aliyun.odps.writer

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.aliyun.odps._
import com.aliyun.odps.data.Record
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelRecordWriter

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.odps.datasource.ODPSOptions
import org.apache.spark.aliyun.odps.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

class ODPSWriter(options: ODPSOptions) extends Serializable with Logging {

  private val project = options.project
  private val table = options.table

  @transient private val odpsUtils: OdpsUtils = options.odpsUtil
  @transient private val tunnel: TableTunnel = odpsUtils.getTableTunnel

  case class RecordWrapper(spec: String, record: Record)

  /**
   * save data [[DataFrame]] to the specified table
   * @param data The data [[DataFrame]] to be saved.
   * @param saveMode [[SaveMode]] specify the expected behavior of saving
    *                a DataFrame to a data source
   * @param dyncPartition currently not supported, default None
   * @return
   */
  def saveToTable(
      data: DataFrame,
      saveMode: SaveMode,
      dyncPartition: Option[Seq[String]] = None): Unit = {

    val schemaMap = data.schema.fieldNames.map(_.toLowerCase).zipWithIndex.toMap

    if (odpsUtils.tableExist(project, table)) {
      // It should be checked after confirming the table is existed whether a table is a
      // partition table or not, otherwise it will throw OdpsException.
      if (saveMode == SaveMode.ErrorIfExists) {
        throw new RuntimeException(
          s"Table $project.$table already exists and SaveMode is ErrorIfExists")
      } else if (saveMode == SaveMode.Ignore) {
        log.warn(s"Table $project.$table already exists and SaveMode is Ignore, No data saved")
        return
      } else if (saveMode == SaveMode.Overwrite) {
        val newSchema = schemaMap.keySet

        val tableSchema = odpsUtils.getTableSchema(project, table)
        val partitionColumns = tableSchema
          .getPartitionColumns.asScala
          .map(_.getName.toLowerCase())
          .toSet
        if (partitionColumns != options.partitionColumns) {
          throw new RuntimeException(
            s"Need partitioned by column ${partitionColumns.mkString(",")}")
        }

        val normalColumns = tableSchema.getColumns.asScala.map(_.getName.toLowerCase).toSet
        val oldSchema = normalColumns ++ partitionColumns

        val diffColumns = (newSchema -- oldSchema) ++ (oldSchema -- newSchema)
        if (diffColumns.nonEmpty) {
          throw new IllegalStateException(
            "Couldn't overwrite table with different schema, please drop table and retry.")
        }

        log.info(s"Table $project.$table will overwrite data when writing.")
      }
    } else {
      val schema = new TableSchema()

      data.schema.fields.foreach { f =>
        val c = new Column(f.name, OdpsUtils.sparkTypeToOdpsType(f.dataType), f.getComment().orNull)
        if (options.partitionColumns.contains(f.name.toLowerCase())) {
          schema.addPartitionColumn(c)
        } else {
          schema.addColumn(c)
        }
      }

      odpsUtils.createTable(project, table, schema, ifNotExists = true)
      options.partitionSpecs.values.foreach { spec =>
        odpsUtils.createPartition(project, table, spec)
      }
    }

    val isPartitionTable = odpsUtils.isPartitionTable(project, table)
    if (isPartitionTable && options.allowCreateNewPartition) {
      options.partitionSpecs.values.foreach(spec => odpsUtils.createPartition(project, table, spec))
    }

    val globalUploadSessions = if (isPartitionTable) {
      options.partitionSpecs.map {
        case (specStr, spec) =>
          val globalUploadSession = tunnel.createUploadSession(project, table, spec, true)
          (specStr, globalUploadSession)
      }
    } else {
      val globalUploadSession = tunnel.createUploadSession(project, table, true)
      Map("all" -> globalUploadSession)
    }
    val partitionSpecToUploadId = globalUploadSessions.map(entry => (entry._1, entry._2.getId))

    data.foreachPartition(it => upload(it, isPartitionTable, schemaMap, partitionSpecToUploadId))

    val arr = Array.tabulate(data.rdd.partitions.length)(l => Long.box(l))
    globalUploadSessions.values.foreach { session =>
      session.commit(arr)
    }
  }

  /**
   * Upload single partition's rows into odps.
   *
   * NOTE: this method need to be serializable.
   * @param rows single partition's data.
   * @param isPartitionTable whether odps table is partition table or not.
   * @param schemaMap to be written table's schema.
   * @param partitionSpecToUploadId a map that key is partitionSpec and value is UploadSession's id.
   */
  private def upload(
      rows: Iterator[Row],
      isPartitionTable: Boolean,
      schemaMap: Map[String, Int],
      partitionSpecToUploadId: Map[String, String]): Unit = {

    val utils = options.odpsUtil
    val tunnel = utils.getTableTunnel

    val partitionIndexes = options.partitionColumns.map { column =>
      schemaMap.getOrElse(column.toLowerCase,
        throw new IllegalArgumentException(s"Table $project.$table didn't contain Column $column."))
    }

    // for avoiding serialize issue.
    val requiredColumnNameToTypeInfo = utils.getTableSchema(project, table)
      .getColumns.asScala
      .filter(column => schemaMap.contains(column.getName.toLowerCase))

    val partitionSpecToUploadResources = partitionSpecToUploadId.map {
      case (spec, uploadId) =>
        val session = if (isPartitionTable) {
          tunnel.getUploadSession(project, table, new PartitionSpec(spec), uploadId)
        } else {
          tunnel.getUploadSession(project, table, uploadId)
        }
        val writer = session.openRecordWriter(TaskContext.get.partitionId, true)
        (spec, (session, writer))
    }

    var recordsWritten = 0L
    var bytesWritten = 0L
    val metrics = TaskContext.get.taskMetrics().outputMetrics

    val queue = new LinkedBlockingQueue[RecordWrapper](options.maxInFlight)
    val finished = new AtomicBoolean(false)

    val writeThread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          while (!finished.get() || !queue.isEmpty) {
            val entry: RecordWrapper = queue.poll(100, TimeUnit.MILLISECONDS)
            if (entry != null) {
              val (_, writer) = partitionSpecToUploadResources.getOrElse(entry.spec,
                throw new RuntimeException(
                  s"Couldn't find any upload session for $project.$table partition ${entry.spec}."))
              writer.write(entry.record)
              recordsWritten += 1
            }
          }
        } catch {
          case _: InterruptedException =>
            logWarning("Writing thread interrupted.")
        } finally {
          partitionSpecToUploadResources.foreach {
            case (_, (_, writer)) =>
              writer.close()
              bytesWritten += writer.asInstanceOf[TunnelRecordWriter].getTotalBytes
          }
        }
      }
    }, "ODPS-Record-Writer")
    writeThread.start()

    try {
      while (rows.hasNext) {
        val row = rows.next()

        val spec = if (isPartitionTable) {
          partitionIndexes
            .map(index => s"${row.schema.fields(index).name}=${row.get(index).toString}")
            .mkString("/")
        } else {
          "all"
        }

        val (uploadSession, _) = partitionSpecToUploadResources.getOrElse(spec,
          throw new RuntimeException(
            s"Couldn't find any upload session for $project.$table partition ${spec}."))

        val record = uploadSession.newRecord()
        requiredColumnNameToTypeInfo.foreach { column =>
          val idx = schemaMap(column.getName.toLowerCase())
          val value = Option(row.get(idx))
            .map(v => OdpsUtils.sparkData2OdpsData(column.getTypeInfo)(v.asInstanceOf[Object]))
            .orNull
          record.set(column.getName, value)
        }

        queue.put(RecordWrapper(spec, record))
      }
    } catch {
      case e: Exception =>
        logError("Error occurs while writing data.", e)
        writeThread.interrupt()
        throw e
    } finally {
      finished.compareAndSet(false, true)
    }
    writeThread.join()

    metrics.setBytesWritten(bytesWritten)
    metrics.setRecordsWritten(recordsWritten)
  }

}
