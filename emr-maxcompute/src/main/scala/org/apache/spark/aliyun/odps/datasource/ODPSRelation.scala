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

import com.aliyun.odps.Odps
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.tunnel.TableTunnel

import org.apache.spark.SparkException
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._

case class ODPSRelation(
    accessKeyId: String,
    accessKeySecret: String,
    odpsUrl: String,
    tunnelUrl: String,
    project: String,
    table: String,
    partitionSpec: String,
    numPartitions: Int)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan with Logging {

  @transient val account = new AliyunAccount(accessKeyId, accessKeySecret)
  @transient val odps = new Odps(account)

  odps.setEndpoint(odpsUrl)
  @transient val tunnel = new TableTunnel(odps)
  tunnel.setEndpoint(tunnelUrl)
  @transient val odpsUtils = new OdpsUtils(odps)

  if (!odpsUtils.tableExist(table, project)) {
    throw new SparkException(s"Table $project.$table didn't exist.")
  }

  private val isPartitionTable = odpsUtils.isPartitionTable(table, project)

  override val needConversion: Boolean = false

  override val schema: StructType = {
    val tableSchema = odpsUtils.getTableSchema(project, table, isPartitionTable)

    StructType(
      tableSchema.map(e => OdpsUtils.getCatalystType(e._1, e._2, nullable = true))
    )
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredSchema = StructType(requiredColumns.map(c => schema.fields(schema.fieldIndex(c))))
    log.info(s"##### table $project.$table partitionSpec is" +
      s" ${Option(partitionSpec).getOrElse("null")} need columns ${requiredColumns.mkString(",")}")
    new ODPSRDD(
      sqlContext.sparkContext,
      requiredSchema,
      accessKeyId,
      accessKeySecret,
      odpsUrl,
      tunnelUrl,
      project,
      table,
      partitionSpec,
      numPartitions).asInstanceOf[RDD[Row]]
  }

  override def toString: String = {
    // credentials should not be included in the plan output, table information is sufficient.
    s"ODPSRelation($table)"
  }

}
