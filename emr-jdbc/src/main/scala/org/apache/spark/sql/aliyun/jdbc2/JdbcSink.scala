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

package org.apache.spark.sql.aliyun.jdbc2

import java.sql.Connection

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.aliyun.jdbc2.JdbcUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.{SparkEnv, TaskContext}

class JdbcSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode) extends Sink with Logging {
  val options = new JDBCOptions(parameters)

  val sinkLog = new JDBCSinkLog(parameters, sqlContext.sparkSession)
  // If user specifies a batchIdCol in the parameters, then it means that the user wants exactly
  // once semantics. This column will store the batch Id for the row when an uncommitted batch
  // is replayed, JDBC Sink will delete the rows that were added to the previous play of the
  // batch
  val batchIdCol = parameters.get("batchIdCol")

  val useTemporaryExecutorTable = isFastLoad()
  logInfo(s"Using temporary executor tables : $useTemporaryExecutorTable")

  def addBatch(batchId: Long, df: DataFrame): Unit = {
    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      // batchIdCol unset means users dont want exactly once semantics, so there is no need
      // to use SinkLog to record batch information.
      if (batchIdCol.isEmpty) {
        addBatchImpl(conn, batchId, df)
      } else {
        val sinkLogConn = sinkLog.createSinkLogConnectionFactory(parameters)()
        if (sinkLog.isBatchCommitted(batchId, sinkLogConn)) {
          logInfo(s"Skipping already committed batch $batchId")
        } else {
          sinkLog.startBatch(batchId, sinkLogConn)
          addBatchImpl(conn, batchId, df)
          sinkLog.commitBatch(batchId, sinkLogConn)
        }
      }
    } finally {
      conn.close()
    }
  }

  private def addBatchImpl(conn: Connection, batchId: Long, df: DataFrame): Unit = {
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis
    val tableExists = JdbcUtils.tableExists(conn, options)
    if (tableExists) {
      if (outputMode == OutputMode.Complete()) {
        if (options.isTruncate
          && isCascadingTruncateTable(options.url).contains(false)) {
          // In this case, we should truncate table and then load.
          truncateTable(conn, options)
          saveRows(df, isCaseSensitive, parameters, batchId)
        } else {
          // Otherwise, do not truncate the table, instead drop and recreate it
          dropTable(conn, getTableName(options))
          createTable(conn, df.schema, df.sparkSession, options)
          saveRows(df, isCaseSensitive, parameters, batchId)
        }
      } else if (outputMode == OutputMode.Append() || outputMode == OutputMode.Update()) {
        saveRows(df, isCaseSensitive, parameters, batchId)
      } else {
        throw new IllegalArgumentException(s"$outputMode not supported")
      }
    } else {
      createTable(conn, df.schema, df.sparkSession, options)
      saveRows(df, isCaseSensitive, parameters, batchId)
    }
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveRows(
      df: DataFrame,
      isCaseSensitive: Boolean,
      parameters: Map[String, String],
      batchId: Long): Unit = {
    if (useTemporaryExecutorTable) {
      saveRowsUsingTemporaryExecutorTable(df, isCaseSensitive, parameters, batchId)
    } else {
      saveRowsToTargetTable(df, isCaseSensitive, options, batchId)
    }
  }

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def saveRowsToTargetTable(
      df: DataFrame,
      isCaseSensitive: Boolean,
      options: JDBCOptions,
      batchId: Long): Unit = {
    val url = options.url
    val table = getTableName(options)
    val dialect = JdbcDialects.get(url)
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 =>
        throw new IllegalArgumentException(
          s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
            "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }
    if (batchIdCol.isEmpty) {

      val insertStmt =
        getStatement(sqlContext.sparkSession.conf, table, df.schema, None, isCaseSensitive, dialect)
      val rddSchema = df.schema
      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
        JdbcUtils.saveInternalPartition(
          getConnection,
          table,
          iterator,
          rddSchema,
          insertStmt,
          batchSize,
          dialect,
          isolationLevel)
      })
    } else {

      // batchId col is defined.. construct a schema by adding the batchId col to the DF schema
      // also put the value of the batch id to the end of every row in the DF
      val dfSchema = df.schema
      val rddSchema: StructType = df.schema.add(batchIdCol.get, LongType, false)
      val insertStmt =
        getStatement(sqlContext.sparkSession.conf, table, rddSchema, None, isCaseSensitive, dialect)
      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
        JdbcUtils.saveInternalPartition(
          getConnection,
          table,
          iterator
            .map(ir => InternalRow.fromSeq(ir.toSeq(dfSchema) :+ batchId)),
          rddSchema,
          insertStmt,
          batchSize,
          dialect,
          isolationLevel)
      })
    }
  }

  def saveRowsUsingTemporaryExecutorTable(
      df: DataFrame,
      isCaseSensitive: Boolean,
      parameters: Map[String, String],
      batchId: Long): Unit = {
    val targetTable = getTableName(options)
    val dialect = JdbcDialects.get(options.url)
    val getConnection: () => Connection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 =>
        throw new IllegalArgumentException(
          s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
            "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    if (batchIdCol.isEmpty) {
      val rddSchema = df.schema
      val sparkSession = df.sparkSession
      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
        val executorTable = targetTable + "$" + SparkEnv.get.executorId + "_" + TaskContext.getPartitionId()
        val executorParameters = parameters + ("dbtable" -> executorTable)
        val executorOptions = new JDBCOptions(executorParameters)
        val conn = createConnectionFactory(executorOptions).apply()
        val tableExists = JdbcUtils.tableExists(conn, executorOptions)
        if (!tableExists) {
          createTable(conn, rddSchema, sparkSession, executorOptions)
        } else {
           if (!isTableEmpty(conn, executorOptions)) {
             throw new IllegalStateException(s"Executor table $executorTable is not empty")
           }
        }
        //Write to executor table
        val insertStmt = getStatement(sqlContext.sparkSession.conf, executorTable, rddSchema, None, isCaseSensitive, dialect)
        JdbcUtils.saveInternalPartition(getConnection,
          executorTable,
          iterator,
          rddSchema,
          insertStmt,
          batchSize,
          dialect,
          isolationLevel)

        //Copy all data from executor table to target table
        val copyStmt = getCopyStatement(executorTable, targetTable, rddSchema, None, dialect)
        JdbcUtils.executeSimpleStatement(getConnection,
          copyStmt,
          isolationLevel)

        //Truncate the executor table
        truncateTable(conn, executorOptions)
      })
    } else {
      // batchId col is defined.. construct a schema by adding the batchId col to the DF schema
      // also put the value of the batch id to the end of every row in the DF
      val dfSchema = df.schema
      val rddSchema: StructType = df.schema.add(batchIdCol.get, LongType, false)
      val sparkSession = df.sparkSession
      repartitionedDF.queryExecution.toRdd.foreachPartition(iterator => {
        val executorTable = targetTable + "$" + SparkEnv.get.executorId + "_" + TaskContext.getPartitionId()
        val executorParameters = parameters + ("dbtable" -> executorTable)
        val executorOptions = new JDBCOptions(executorParameters)
        val conn = createConnectionFactory(executorOptions).apply()
        val tableExists = JdbcUtils.tableExists(conn, executorOptions)
        if (!tableExists) {
          createTable(conn, rddSchema, sparkSession, executorOptions)
        } else {
          if (!isTableEmpty(conn, executorOptions)) {
            throw new IllegalStateException(s"Executor table $executorTable is not empty")
          }
        }
        //Write to executor table
        val insertStmt = getStatement(sqlContext.sparkSession.conf, executorTable, rddSchema, None, isCaseSensitive, dialect)
        JdbcUtils.saveInternalPartition(getConnection,
          executorTable,
          iterator.map(ir => InternalRow.fromSeq(ir.toSeq(dfSchema) :+ batchId)),
          rddSchema,
          insertStmt,
          batchSize,
          dialect,
          isolationLevel)

        //Copy all data from executor table to target table
        val copyStmt = getCopyStatement(executorTable, targetTable, rddSchema, None, dialect)
        JdbcUtils.executeSimpleStatement(getConnection,
          copyStmt,
          isolationLevel)

        //Truncate the executor table
        truncateTable(conn, executorOptions)
      })
    }
  }

  def isFastLoad() : Boolean = {
    options.url.contains("TYPE=FASTLOAD")
  }

}
