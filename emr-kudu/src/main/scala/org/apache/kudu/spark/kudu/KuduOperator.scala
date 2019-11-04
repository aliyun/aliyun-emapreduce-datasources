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

import org.apache.kudu.client.{AsyncKuduClient, KuduClient, KuduSession, RowErrorsAndOverflowStatus}
import org.apache.kudu.client.SessionConfiguration.FlushMode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.StructType

class KuduOperator(kuduMaster: String) extends Logging with Serializable {
  @transient lazy val syncClient: KuduClient = asyncClient.syncClient()

  @transient lazy val asyncClient: AsyncKuduClient = KuduClientCache.getAsyncClient(kuduMaster)

  def writeRows(
      data: Dataset[Row],
      schema: StructType,
      opTypeColumnName: String,
      tableName: String,
      writeOptions: KuduWriteOptions): Unit = {
    val syncClient: KuduClient = KuduClientCache.getAsyncClient(kuduMaster).syncClient()
    val lastPropagatedTimestamp = syncClient.getLastPropagatedTimestamp
    data.toDF().foreachPartition(it => {
      val operator = new KuduOperator(kuduMaster)
      operator.writePartitionRows(it, schema, opTypeColumnName, tableName, lastPropagatedTimestamp, writeOptions)
    })
  }

  private def writePartitionRows(
      rows: Iterator[Row],
      schema: StructType,
      opTypeColumnName: String,
      tableName: String,
      lastPropagatedTimestamp: Long,
      writeOptions: KuduWriteOptions): RowErrorsAndOverflowStatus = {
    // Since each executor has its own KuduClient, update executor's propagated timestamp
    // based on the last one on the driver.
    syncClient.updateLastPropagatedTimestamp(lastPropagatedTimestamp)
    val table = syncClient.openTable(tableName)
    val rowConverter = new RowConverter(table.getSchema, schema, writeOptions.ignoreNull)
    val session: KuduSession = syncClient.newSession
    session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
    session.setIgnoreAllDuplicateRows(writeOptions.ignoreDuplicateRowErrors)
    var numRows = 0
    try {
      for (row <- rows) {
        val opType = getOperationType(row.getAs[String](opTypeColumnName))
        val partialRow = rowConverter.toPartialRow(row)
        val operation = opType.operation(table)
        operation.setRow(partialRow)
        session.apply(operation)
        numRows += 1
      }
    } finally {
      session.close()
    }
    session.getPendingErrors
  }

  private def getOperationType(operationTag: String): OperationType = {
    operationTag match {
      case "delete" =>
        KuduSourceProvider.stringToOperationType("delete")
      case "insert" =>
        KuduSourceProvider.stringToOperationType("insert")
      case "update" =>
        KuduSourceProvider.stringToOperationType("update")
    }
  }
}
