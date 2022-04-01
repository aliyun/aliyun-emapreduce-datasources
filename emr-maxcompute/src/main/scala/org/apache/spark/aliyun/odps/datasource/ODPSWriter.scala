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

import com.aliyun.odps._
import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.tunnel.TableTunnel
import org.slf4j.LoggerFactory
import org.apache.spark.TaskContext
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

class ODPSWriter(
    accessKeyId: String,
    accessKeySecret: String,
    odpsUrl: String,
    tunnelUrl: String) extends Serializable {

  @transient val account = new AliyunAccount(accessKeyId, accessKeySecret)
  @transient val odps = new Odps(account)

  odps.setEndpoint(odpsUrl)
  @transient val tunnel = new TableTunnel(odps)
  tunnel.setEndpoint(tunnelUrl)
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
    dyncPartition: Option[Seq[String]] = None) {
    odps.setDefaultProject(project)

    val tableExists = odpsUtils.tableExist(table, project)
    val isPartitionTable = odpsUtils.isPartitionTable(table, project)

    if (isPartitionTable && partitionSpec == null) {
      sys.error(s"when $project.$table is a partition table, you should provide option " +
        "'partitionSpec'")
    }

    val shouldUpload = {
      if (saveMode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"$project.$table ${if (isPartitionTable) partitionSpec else ""} " +
          s"already exists and SaveMode is ErrorIfExists")
      } else if (saveMode == SaveMode.Ignore && tableExists) {
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
        true
      } else true
    }

    if (shouldUpload) {
      if (isPartitionTable && defaultCreate) {
        odpsUtils.createPartition(project, table, partitionSpec)
      }

      val account = new AliyunAccount(accessKeyId, accessKeySecret)
      val odps = new Odps(account)
      odps.setDefaultProject(project)
      odps.setEndpoint(odpsUrl)
      val tunnel = new TableTunnel(odps)
      tunnel.setEndpoint(tunnelUrl)
      val uploadSession = if (isPartitionTable) {
        val parSpec = new PartitionSpec(partitionSpec)
        tunnel.createUploadSession(project, table, parSpec)
      } else {
        tunnel.createUploadSession(project, table)
      }
      val uploadId = uploadSession.getId

      def writeToFile(odps: Odps, schema: Array[(String, TypeInfo)], iter: Iterator[Row]) {
        val tunnel_ = new TableTunnel(odps)
        tunnel_.setEndpoint(tunnelUrl)
        val uploadSession_ = if (isPartitionTable) {
          val parSpec = new PartitionSpec(partitionSpec)
          tunnel_.getUploadSession(project, table, parSpec, uploadId)
        } else {
          tunnel_.getUploadSession(project, table, uploadId)
        }

        val writer = uploadSession_.openRecordWriter(TaskContext.get.partitionId)

        var recordsWritten = 0L

        while (iter.hasNext) {
          val value = iter.next()
          val record = uploadSession_.newRecord()

          schema.zipWithIndex.foreach {
            case (s: (String, TypeInfo), idx: Int) =>
              try {
                record.set(s._1, OdpsUtils.sparkData2OdpsData(s._2)(value.get(idx).asInstanceOf[Object]))
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

      data.foreachPartition {
        iterator =>
          val account_ = new AliyunAccount(accessKeyId, accessKeySecret)
          val odps = new Odps(account_)
          odps.setDefaultProject(project)
          odps.setEndpoint(odpsUrl)
          val odpsUtils = new OdpsUtils(odps)
          val dataSchema = odpsUtils.getTableSchema(project, table, false)
          writeToFile(odps, dataSchema, iterator)
      }
      val arr = Array.tabulate(data.rdd.partitions.length)(l => Long.box(l))
      uploadSession.commit(arr)
    }
 }
}
