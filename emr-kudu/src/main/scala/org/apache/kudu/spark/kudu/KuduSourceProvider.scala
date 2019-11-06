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

package org.apache.kudu.spark.kudu

import java.net.InetAddress

import scala.util.Try

import org.apache.kudu.client.ReplicaSelection
import org.apache.kudu.spark.kudu.KuduReadOptions.{defaultBatchSize, defaultFaultTolerantScanner, defaultKeepAlivePeriodMs, defaultScanLocality}
import org.apache.kudu.spark.kudu.KuduWriteOptions.{defaultIgnoreNull, defaultRepartition, defaultRepartitionSort}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class KuduSourceProvider extends DefaultSource with Logging {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {

    val tableName = parameters.getOrElse(
      TABLE_KEY,
      throw new IllegalArgumentException(
        s"Kudu table name must be specified in create options using key '$TABLE_KEY'"))
    val kuduMaster = parameters.getOrElse(KUDU_MASTER, InetAddress.getLocalHost.getCanonicalHostName)
    val operationType = parameters.get(OPERATION).map(KuduSourceProvider.stringToOperationType).getOrElse(Upsert)
    val schemaOption = Option(schema)
    val batchSize = parameters.get(BATCH_SIZE).map(_.toInt).getOrElse(defaultBatchSize)
    val faultTolerantScanner =
      parameters.get(FAULT_TOLERANT_SCANNER).map(_.toBoolean).getOrElse(defaultFaultTolerantScanner)
    val scanLocality =
      parameters.get(SCAN_LOCALITY).map(KuduSourceProvider.getScanLocalityType).getOrElse(defaultScanLocality)
    val scanRequestTimeoutMs = parameters.get(SCAN_REQUEST_TIMEOUT_MS).map(_.toLong)
    val keepAlivePeriodMs =
      parameters.get(KEEP_ALIVE_PERIOD_MS).map(_.toLong).getOrElse(defaultKeepAlivePeriodMs)
    val splitSizeBytes = parameters.get(SPLIT_SIZE_BYTES).map(_.toLong)

    val readOptions = KuduReadOptions(
      batchSize,
      scanLocality,
      faultTolerantScanner,
      keepAlivePeriodMs,
      scanRequestTimeoutMs,
      /* socketReadTimeoutMs= */ None,
      splitSizeBytes)

    val ignoreDuplicateRowErrors =
      Try(parameters(IGNORE_DUPLICATE_ROW_ERRORS).toBoolean).getOrElse(false) ||
        Try(parameters(OPERATION) == "insert-ignore").getOrElse(false)
    val ignoreNull =
      parameters.get(IGNORE_NULL).map(_.toBoolean).getOrElse(defaultIgnoreNull)
    val repartition =
      parameters.get(REPARTITION).map(_.toBoolean).getOrElse(defaultRepartition)
    val repartitionSort =
      parameters.get(REPARTITION_SORT).map(_.toBoolean).getOrElse(defaultRepartitionSort)
    val writeOptions = KuduWriteOptions(ignoreDuplicateRowErrors, ignoreNull, repartition, repartitionSort)

    new KuduUpdatableRelation(tableName, kuduMaster, operationType, schemaOption, readOptions, writeOptions)(sqlContext)
  }
}

object KuduSourceProvider {
  def stringToOperationType(opParam: String): OperationType = {
    opParam.toLowerCase match {
      case "insert" => Insert
      case "insert-ignore" => Insert
      case "upsert" => Upsert
      case "update" => Update
      case "delete" => Delete
      case _ =>
        throw new IllegalArgumentException(s"Unsupported operation type '$opParam'")
    }
  }

  def getScanLocalityType(opParam: String): ReplicaSelection = {
    opParam.toLowerCase match {
      case "leader_only" => ReplicaSelection.LEADER_ONLY
      case "closest_replica" => ReplicaSelection.CLOSEST_REPLICA
      case _ =>
        throw new IllegalArgumentException(s"Unsupported replica selection type '$opParam'")
    }
  }
}
