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

import java.sql.{Date, SQLException}

import com.aliyun.odps._
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.Binary
import com.aliyun.odps.tunnel.TableTunnel
import org.slf4j.LoggerFactory

import org.apache.spark.TaskContext
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types._

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

      def writeToFile(schema: Array[(String, OdpsType)], iter: Iterator[Row]) {
        val account_ = new AliyunAccount(accessKeyId, accessKeySecret)
        val odps_ = new Odps(account_)
        odps_.setDefaultProject(project)
        odps_.setEndpoint(odpsUrl)
        val tunnel_ = new TableTunnel(odps_)
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
            case (s: (String, OdpsType), idx: Int) =>
              try {
                s._2 match {
                  case OdpsType.BIGINT =>
                    record.setBigint(s._1, value.get(idx).toString.toLong)
                  case OdpsType.BINARY =>
                    record.set(s._1, new Binary(value.getAs[Array[Byte]](idx)))
                  case OdpsType.BOOLEAN =>
                    record.setBoolean(s._1, value.getBoolean(idx))
                  case OdpsType.CHAR =>
                    record.set(s._1, new com.aliyun.odps.data.Char(value.get(idx).toString))
                  case OdpsType.DATE =>
                    record.set(s._1, value.getAs[Date](idx))
                  case OdpsType.DATETIME =>
                    record.set(s._1, new java.util.Date(value.getAs[Date](idx).getTime))
                  case OdpsType.DECIMAL =>
                    record.set(s._1, Decimal(value.get(idx).toString).toJavaBigDecimal)
                  case OdpsType.DOUBLE =>
                    record.setDouble(s._1, value.getDouble(idx))
                  case OdpsType.FLOAT =>
                    record.set(s._1, value.get(idx).toString.toFloat)
                  case OdpsType.INT =>
                    record.set(s._1, value.get(idx).toString.toInt)
                  case OdpsType.SMALLINT =>
                    record.set(s._1, value.get(idx).toString.toShort)
                  case OdpsType.STRING =>
                    record.setString(s._1, value.get(idx).toString)
                  case OdpsType.TINYINT =>
                    record.set(s._1, value.getAs[Byte](idx))
                  case OdpsType.VARCHAR =>
                    record.set(s._1, new com.aliyun.odps.data.Varchar(value.get(idx).toString))
                  case OdpsType.TIMESTAMP =>
                    record.setDatetime(s._1, value.getAs[java.sql.Timestamp](idx))
                  case OdpsType.VOID => record.set(s._1, null)
                  case OdpsType.INTERVAL_DAY_TIME =>
                    throw new SQLException(s"Unsupported type 'INTERVAL_DAY_TIME'")
                  case OdpsType.INTERVAL_YEAR_MONTH =>
                    throw new SQLException(s"Unsupported type 'INTERVAL_YEAR_MONTH'")
                  case OdpsType.MAP =>
                    throw new SQLException(s"Unsupported type 'MAP'")
                  case OdpsType.STRUCT =>
                    throw new SQLException(s"Unsupported type 'STRUCT'")
                  case OdpsType.ARRAY =>
                    throw new SQLException(s"Unsupported type 'ARRAY'")
                  case _ => throw new SQLException(s"Unsupported type ${s._2}")
                }
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

      val dataSchema = odpsUtils.getTableSchema(project, table, false)
        .map{ e => (e._1, e._2.getOdpsType) }
      data.foreachPartition {
        iterator: Iterator[Row] =>
          writeToFile(dataSchema, iterator)
      }
      val arr = Array.tabulate(data.rdd.partitions.length)(l => Long.box(l))
      uploadSession.commit(arr)
    }
 }
}
