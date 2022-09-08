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
import scala.collection.immutable.HashSet

import com.aliyun.odps.PartitionSpec

import org.apache.spark.aliyun.odps.utils.OdpsUtils
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.util.Utils

class ODPSOptions(parameters: Map[String, String])
  extends Serializable {

  val writeBufferSize: Long = Utils.byteStringAsMb(parameters.getOrElse("writeBufferSize", "64M"))
  val flushThreshold: Double = parameters.getOrElse("flushThreshold", "0.9").toDouble
  val maxInFlight: Int = parameters.getOrElse("maxInFlight", "256").toInt

  // Aliyun Account accessKeyId
  val accessKeyId: String =
    parameters.getOrElse("accessKeyId", sys.error("Option 'accessKeyId' not specified"))

  // Aliyun Account accessKeySecret
  val accessKeySecret: String =
    parameters.getOrElse("accessKeySecret", sys.error("Option 'accessKeySecret' not specified"))

  // the odps endpoint URL
  val odpsUrl: String =
    parameters.getOrElse("odpsUrl", sys.error("Option 'odpsUrl' not specified"))

  // the TableTunnel endpoint URL
  val tunnelUrl: String =
    parameters.getOrElse("tunnelUrl", sys.error("Option 'tunnelUrl' not specified"))

  @transient
  lazy val odpsUtil: OdpsUtils = OdpsUtils(accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)

  // the project name
  val project: String = parameters.getOrElse("project", sys.error("Option 'project' not specified"))

  // the table name
  val table: String = parameters.getOrElse("table", sys.error("Option 'table' not specified"))

  // describe the partition of the table, like pt=xxx/dt=xxx,pt=yyy/dt=yyy
  val partitions: Set[String] = parameters.get("partitionSpec")
    .map(_.split(",").toSet)
    .getOrElse(new HashSet[String]())

  @transient
  val partitionSpecs: Map[String, PartitionSpec] = partitions
    .map(spec => (spec, new PartitionSpec(spec))).toMap

  // if allowed to create the specific partition which does not exist in table
  val allowCreateNewPartition: Boolean = parameters.getOrElse("allowCreateNewPartition", "true").toBoolean

  // spark.write.partitionBy(columns)
  val partitionColumns: Set[String] = parameters.get(DataSourceUtils.PARTITIONING_COLUMNS_KEY)
    .map(DataSourceUtils.decodePartitioningColumns)
    .map(_.map(_.toLowerCase()).toSet)
    .getOrElse(new HashSet[String]())

  validate()

  private def validate(): Unit = {
    // not a partitioned table.
    if (partitionColumns.isEmpty && partitions.isEmpty) {
      return
    }

    // todo: remove `partitionSpec` option.
    if (partitionColumns.nonEmpty && partitions.isEmpty ||
        partitionColumns.isEmpty && partitions.nonEmpty) {
      throw new IllegalArgumentException("Please use `Dataframe.write.partitionBy().save`" +
        " and provide `partitionSpec` at the same time.")
    }

    val providedPartitionColumns = partitionSpecs.values.flatMap(_.keys().asScala).toSet
    if (partitionColumns != providedPartitionColumns) {
      throw new IllegalArgumentException(s"Please provide `partitionSpec` with all partitions" +
        s" specified by `Dataframe.write.partitionBy().save`, current partitionSpecs columns" +
        s" ${providedPartitionColumns.mkString(",")}, partitionBy columns" +
        s" ${partitionColumns.mkString(",")}.")
    }
  }

}
