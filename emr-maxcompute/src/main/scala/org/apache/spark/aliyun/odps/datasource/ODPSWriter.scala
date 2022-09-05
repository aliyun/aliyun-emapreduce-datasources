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

import scala.collection.JavaConverters._

import com.aliyun.odps._
import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

class ODPSWriter(options: ODPSOptions) extends Serializable with Logging {

  @transient val odpsUtils: OdpsUtils = options.odpsUtil
  @transient val tunnel: TableTunnel = odpsUtils.getTableTunnel

  private val project = options.project
  private val table = options.table

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
        log.info(s"Table $project.$table will overwrite data when writing.")
      }

      val partitionColumns = odpsUtils.getTableSchema(project, table)
        .getPartitionColumns.asScala
        .map(_.getName.toLowerCase())
        .toSet

      if (partitionColumns != options.partitionColumns) {
        throw new RuntimeException(s"Need partitioned by column ${partitionColumns.mkString(",")}")
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

    upload(data)
  }

  private def upload(data: DataFrame): Unit = {
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

    val schemaMap = data.schema.fields.zipWithIndex.map {
      case (field, index) => (field.name.toLowerCase, index)
    }.toMap
    val partitionIndexes = options.partitionColumns.map { column =>
        schemaMap.getOrElse(column.toLowerCase,
          throw new IllegalArgumentException(
            s"Table $project.$table didn't contain Column $column."))
    }

    data.foreachPartition { iterator =>
      val utils = options.odpsUtil
      val tunnel = utils.getTableTunnel
      val dataSchema = utils.getTableSchema(project, table, isPartition = false)

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

      while (iterator.hasNext) {
        val row = iterator.next()

        val spec = if (isPartitionTable) {
          partitionIndexes
            .map(index => s"${row.schema.fields(index).name}=${row.get(index).toString}")
            .mkString("/")
        } else {
          "all"
        }

        val (uploadSession, writer) = partitionSpecToUploadResources.getOrElse(spec,
          throw new RuntimeException(
            s"Couldn't find any upload session for $project.$table partition $spec."))

        val record = uploadSession.newRecord()

        dataSchema.zipWithIndex.foreach {
          case (s: (String, TypeInfo), idx: Int) =>
            try {
              val value = OdpsUtils.sparkData2OdpsData(s._2)(row.get(idx).asInstanceOf[Object])
              record.set(s._1, value)
            } catch {
              case e: NullPointerException =>
                if (row.get(idx) == null) {
                  record.set(s._1, null)
                } else {
                  throw e
                }
            }
        }
        writer.write(record)
        recordsWritten += 1
      }

      partitionSpecToUploadResources.foreach {
        case (_, (_, writer)) =>
          writer.close()
          writer match {
            case w: TunnelBufferedWriter =>
              bytesWritten += w.getTotalBytes
            case w: ProtobufRecordStreamWriter =>
              bytesWritten += w.getTotalBytes
          }
      }

      metrics.setBytesWritten(bytesWritten)
      metrics.setRecordsWritten(recordsWritten)
    }

    val arr = Array.tabulate(data.rdd.partitions.length)(l => Long.box(l))
    globalUploadSessions.values.foreach { session =>
      session.commit(arr)
    }
  }

}
