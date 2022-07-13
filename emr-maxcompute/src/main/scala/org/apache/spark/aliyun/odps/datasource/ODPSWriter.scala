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

import com.aliyun.odps._
import com.aliyun.odps.`type`.{TypeInfo, TypeInfoFactory}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.tunnel.TableTunnel
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

class ODPSWriter(odpsOptions: ODPSOptions) extends Serializable {

  @transient val account = new AliyunAccount(odpsOptions.accessKeyId, odpsOptions.accessKeySecret)
  @transient val odps = new Odps(account)

  odps.setEndpoint(odpsOptions.odpsUrl)
  @transient val tunnel = new TableTunnel(odps)
  tunnel.setEndpoint(odpsOptions.tunnelUrl)
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

    val tableExists = odpsUtils.tableExist(table, project)

    if (tableExists) {
      // It should be checked after confirming the table is existed whether a table is a
      // partition table or not, otherwise it will throw OdpsException.
      val isPartitionTable = odpsUtils.isPartitionTable(table, project)
      if (saveMode == SaveMode.ErrorIfExists) {
        sys.error(s"$project.$table ${if (isPartitionTable) partitionSpec else ""} " +
          s"already exists and SaveMode is ErrorIfExists")
      } else if (saveMode == SaveMode.Ignore) {
        log.info(s"Table $project.$table already exists and SaveMode is Ignore, No data saved")
        return
      } else if (saveMode == SaveMode.Overwrite) {
        if (!isPartitionTable) {
          log.info(s"SaveMode.Overwrite with no-partition Table, " +
            s"truncate the $project.$table first")
          odpsUtils.runSQL(project, s"TRUNCATE TABLE $table;")
        } else {
          log.info(s"SaveMode.Overwrite with partition Table, " +
            s"drop the $partitionSpec of $project.$table first")
          odpsUtils.dropPartition(project, table, partitionSpec)
          odpsUtils.createPartition(project, table, partitionSpec)
        }
      }
    } else {
      val schema = new TableSchema()

      // for `dataframe.write.partitionBy()`
      val partitionColumns = new util.HashSet[String]()
      if (odpsOptions.partitionColumns != null) {
        partitionColumns.addAll(odpsOptions.partitionColumns.asJava)
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
        odpsUtils.createPartition(project, table, partitionSpec)
      } else if (partitionSpec != null) {
        log.warn(s"Table $project.$table is not a partition table," +
          s" partitionSpec $partitionSpec will be ignored.")
      }
    }

    upload(project, table, data, partitionSpec, defaultCreate)
  }

  private def writeToFile(
      uploadSession: TableTunnel#UploadSession,
      schema: Array[(String, TypeInfo)],
      iter: Iterator[Row]): Unit = {
    val writer = uploadSession.openRecordWriter(TaskContext.get.partitionId)
    var recordsWritten = 0L

    while (iter.hasNext) {
      val value = iter.next()
      val record = uploadSession.newRecord()

      schema.zipWithIndex.foreach {
        case (s: (String, TypeInfo), idx: Int) =>
          try {
            record.set(s._1,
              OdpsUtils.sparkData2OdpsData(s._2)(value.get(idx).asInstanceOf[Object]))
          } catch {
            case e: NullPointerException =>
              if (value.get(idx) == null) {
                record.set(s._1, null)
              } else {
                throw e
              }
          }
      }
      writer.write(record)
      recordsWritten += 1
    }

    writer.close()
  }

  private def upload(
      project: String,
      table: String,
      data: DataFrame,
      partitionSpec: String,
      defaultCreate: Boolean): Unit = {
    val isPartitionTable = odpsUtils.isPartitionTable(table, project)
    if (isPartitionTable && defaultCreate) {
      odpsUtils.createPartition(project, table, partitionSpec)
    }

    val uploadSession = if (isPartitionTable) {
      val parSpec = new PartitionSpec(partitionSpec)
      tunnel.createUploadSession(project, table, parSpec)
    } else {
      tunnel.createUploadSession(project, table)
    }
    val uploadId = uploadSession.getId

    data.foreachPartition((iterator: Iterator[Row]) => {
      val account = new AliyunAccount(odpsOptions.accessKeyId, odpsOptions.accessKeySecret)

      val odps = new Odps(account)
      odps.setDefaultProject(project)
      odps.setEndpoint(odpsOptions.odpsUrl)

      val odpsUtils = new OdpsUtils(odps)
      val dataSchema = odpsUtils.getTableSchema(project, table, isPartition = false)

      val tunnel = new TableTunnel(odps)
      tunnel.setEndpoint(odpsOptions.tunnelUrl)

      val uploadSession = if (isPartitionTable) {
        val parSpec = new PartitionSpec(partitionSpec)
        tunnel.getUploadSession(project, table, parSpec, uploadId)
      } else {
        tunnel.getUploadSession(project, table, uploadId)
      }

      writeToFile(uploadSession, dataSchema, iterator)
    })
    val arr = Array.tabulate(data.rdd.partitions.length)(l => Long.box(l))
    uploadSession.commit(arr)
  }

}
