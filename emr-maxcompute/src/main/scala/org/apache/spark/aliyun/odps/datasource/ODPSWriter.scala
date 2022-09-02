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

import java.util

import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet

import com.aliyun.odps._
import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

class ODPSWriter(options: ODPSOptions) extends Serializable {

  @transient val account = new AliyunAccount(options.accessKeyId, options.accessKeySecret)
  @transient val odps = new Odps(account)

  odps.setEndpoint(options.odpsUrl)
  @transient val tunnel = new TableTunnel(odps)
  tunnel.setEndpoint(options.tunnelUrl)
  @transient val odpsUtils = new OdpsUtils(odps)

  private val log = LoggerFactory.getLogger(getClass)

  /**
   * save data [[DataFrame]] to the specified table
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param data The data [[DataFrame]] to be saved.
   * @param partitionSpec The partition specific, like `pt='xxx',ds='yyy'`.
   * @param defaultCreate Implying whether to create a table partition, if
   * specific partition does not exist.
   * @param saveMode [[SaveMode]] specify the expected behavior of saving
    *                a DataFrame to a data source
   * @param dyncPartition currently not supported, default None
   * @return
   */
  def saveToTable(
      project: String,
      table: String,
      data: DataFrame,
      partitionSpec: String,
      defaultCreate: Boolean,
      saveMode: SaveMode,
      dyncPartition: Option[Seq[String]] = None): Unit = {
    odps.setDefaultProject(project)

    val partitionColumns = if (odpsUtils.tableExist(table, project)) {
      // It should be checked after confirming the table is existed whether a table is a
      // partition table or not, otherwise it will throw OdpsException.
      if (saveMode == SaveMode.ErrorIfExists) {
        sys.error(s"$project.$table partition ${Option(partitionSpec).getOrElse("")} " +
          s"already exists and SaveMode is ErrorIfExists")
      } else if (saveMode == SaveMode.Ignore) {
        log.warn(s"Table $project.$table already exists and SaveMode is Ignore, No data saved")
        return
      } else if (saveMode == SaveMode.Overwrite) {
        log.info(s"Table $project.$table will overwrite data when writing.")
        Option(partitionSpec).foreach(_.split(",").foreach { spec =>
          odpsUtils.createPartitionIfNotExist(project, table, spec)
        })
      }
      Option(odpsUtils.getTableSchema(project, table)
        .getPartitionColumns.asScala
        .map(column => column.getName)
        .toSet)
    } else {
      val schema = new TableSchema()

      // for `dataframe.write.partitionBy()`
      val partitionColumns = new util.HashSet[String]()
      if (options.partitionColumns != null) {
        partitionColumns.addAll(options.partitionColumns.asJava)
      }

      if (!partitionColumns.isEmpty && (partitionSpec == null || partitionSpec.isEmpty)) {
        // todo: support `partitionBy` without providing partitionSpec
        sys.error(s"when $project.$table is a partition table, you should provide option " +
          "'partitionSpec'")
      }

      data.schema.fields.foreach(f => {
        val c = new Column(f.name, OdpsUtils.sparkTypeToOdpsType(f.dataType), f.getComment().orNull)
        if (partitionColumns.contains(f.name.toLowerCase())) {
          schema.addPartitionColumn(c)
        } else {
          schema.addColumn(c)
        }
      })

      odpsUtils.createTable(project, table, schema, ifNotExists = true)
      if (!partitionColumns.isEmpty) {
        partitionSpec.split(",").foreach(spec => odpsUtils.createPartition(project, table, spec))
      } else if (partitionSpec != null) {
        log.warn(s"Table $project.$table is not a partition table," +
          s" partitionSpec $partitionSpec will be ignored.")
      }

      Option(partitionColumns.asScala.toSet)
    }

    upload(project, table, data, Option(partitionSpec), partitionColumns, defaultCreate)
  }

  private def upload(
      project: String,
      table: String,
      data: DataFrame,
      partitionSpec: Option[String],
      partitionColumns: Option[Set[String]],
      defaultCreate: Boolean): Unit = {
    val isPartitionTable = odpsUtils.isPartitionTable(table, project)
    val partitions = partitionSpec.map(_.split(","))

    if (isPartitionTable && defaultCreate) {
      partitions.getOrElse(throw new IllegalArgumentException(
        s"Table $project.$table is partition table, but didn't provide partitionSpec."))
        .foreach(spec => odpsUtils.createPartition(project, table, spec))
    }

    val globalUploadSessions = partitions.map(_.map { spec =>
        val globalUploadSession = tunnel.createUploadSession(
          project, table, new PartitionSpec(spec), true)
        (spec, globalUploadSession)
      }.toMap
    ).getOrElse {
      val globalUploadSession = tunnel.createUploadSession(project, table, true)
      Map("all" -> globalUploadSession)
    }

    val partitionSpecToUploadId = globalUploadSessions.map(entry => (entry._1, entry._2.getId))

    val partitionIndexes = partitionColumns.map { columns =>
      val schemaMap = data.schema.fieldNames.map(name => (name.toLowerCase, name)).toMap
      columns.map { column =>
        val columnName = schemaMap.getOrElse(column.toLowerCase,
          throw new IllegalArgumentException(
            s"Table $project.$table didn't contain Column $column."))
        data.schema.fieldIndex(columnName)
      }
    }.getOrElse(new HashSet[Int]())

    data.foreachPartition { iterator =>
      val utils = OdpsUtils(options.accessKeyId, options.accessKeySecret, options.odpsUrl)
      val tunnel = utils.getTableTunnel(options.tunnelUrl)
      val dataSchema = utils.getTableSchema(project, table, isPartition = false)

      val partitionSpecToUploadResources = partitionSpecToUploadId.map {
        case (spec, uploadId) =>
          val session = if (isPartitionTable) {
            tunnel.getUploadSession(project, table, new PartitionSpec(spec), uploadId)
          } else {
            tunnel.getUploadSession(project, table, uploadId)
          }
          val writer = session.openRecordWriter(TaskContext.get.partitionId)
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
