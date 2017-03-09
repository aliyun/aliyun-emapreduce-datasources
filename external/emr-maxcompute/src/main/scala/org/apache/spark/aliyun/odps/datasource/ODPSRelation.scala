/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{PrunedFilteredScan, Filter, BaseRelation}

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
  with PrunedFilteredScan {

  @transient val account = new AliyunAccount(accessKeyId, accessKeySecret)
  @transient val odps = new Odps(account)

  odps.setEndpoint(odpsUrl)
  @transient val tunnel = new TableTunnel(odps)
  tunnel.setEndpoint(tunnelUrl)
  @transient val odpsUtils = new OdpsUtils(odps)

  override val needConversion: Boolean = false

  override val schema: StructType = {
    val tableSchema = odpsUtils.getTableSchema(project, table, false)

    StructType(
      tableSchema.map(e => {
        e._2 match {
          case "BIGINT" => StructField(e._1, LongType, true)
          case "STRING" => StructField(e._1, StringType, true)
          case "DOUBLE" => StructField(e._1, DoubleType, true)
          case "BOOLEAN" => StructField(e._1, BooleanType, true)
          case "DATETIME" => StructField(e._1, TimestampType, true)
        }
      })
    )
  }

  private val isPartitionTable = odpsUtils.isPartitionTable(table, project)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    val parSpec = if (!isPartitionTable) "Non-Partitioned"
    else if (isPartitionTable && partitionSpec == null) "all"
    else partitionSpec

    val requiredSchema = StructType(requiredColumns.map(c => schema.fields(schema.fieldIndex(c))))
    val rdd = if (!parSpec.equals("all")) {
      new ODPSRDD(sqlContext.sparkContext,
        requiredSchema, accessKeyId,
        accessKeySecret,
        odpsUrl,
        tunnelUrl,
        project,
        table,
        parSpec,
        numPartitions)
    } else {
      val parts = odpsUtils.getAllPartitionSpecs(table, project)
      if (parts.nonEmpty) {
        parts.map {
          ptSpec =>
            new ODPSRDD(sqlContext.sparkContext,
              requiredSchema, accessKeyId,
              accessKeySecret,
              odpsUrl,
              tunnelUrl,
              project,
              table,
              ptSpec.toString,
              numPartitions)

        }.map(_.asInstanceOf[RDD[Row]]).reduce((r1, r2) => r1.union(r2))
      } else sqlContext.sparkContext.emptyRDD
    }

    rdd.asInstanceOf[RDD[Row]]
  }

  override def toString: String = {
    // credentials should not be included in the plan output, table information is sufficient.
    s"ODPSRelation($table)"
  }

}
