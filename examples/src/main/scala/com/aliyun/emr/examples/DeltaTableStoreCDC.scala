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

package com.aliyun.emr.examples

import java.util.UUID

import io.delta.tables.DeltaTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DataTypes, StructField}

object DeltaTableStoreCDC extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      log.error(
        "Usage: DeltaTableStoreCDC <ots-instanceName>" +
          "<ots-tableName> <ots-tunnelId> <access-key-id> <access-key-secret> <ots-endpoint>" +
          "<max-offsets-per-channel> [<checkpoint-location>]"
      )
    }

    val Array(
      instanceName,
      tableName,
      tunnelId,
      accessKeyId,
      accessKeySecret,
      endpoint,
      maxOffsetsPerChannel,
      _*
    ) = args

    val checkpointLocation =
      if (args.length > 7) args(7) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark =
      SparkSession.builder
        .appName("DeltaTableStoreCDC")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()

    spark.readStream
      .format("tablestore")
      .option("instance.name", instanceName)
      .option("table.name", tableName)
      .option("tunnel.id", tunnelId)
      .option("endpoint", endpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("maxOffsetsPerChannel", maxOffsetsPerChannel) // default 10000
      .option(
        "catalog",
        """
          |{"columns": {
          |   "PkString": {"type": "string"}, "PkInt": {"type": "long"},
          |   "col1": {"type": "string"}, "col2": {"type": "long"}, "col3": {"type": "binary"},
          |   "timestamp": {"type": "long"}, "col5": {"type": "double"}, "col6": {"type": "boolean"}
          |}}
          |""".stripMargin
      )
      .load()
      .createTempView("stream")

    // PrimaryKey StructType
    val primaryStructType = DataTypes.createStructType(
      Array[StructField](
        DataTypes.createStructField("PkString", DataTypes.StringType, false),
        DataTypes.createStructField("PkInt", DataTypes.LongType, false)
      )
    )
    // AttributeColumn StructType
    val attributeStructType = DataTypes.createStructType(
      Array[StructField](
        // Attribute Column Fields
        DataTypes.createStructField("col1", DataTypes.StringType, true),
        DataTypes.createStructField("col2", DataTypes.IntegerType, true),
        DataTypes.createStructField("col3", DataTypes.BinaryType, true),
        DataTypes.createStructField("timestamp", DataTypes.IntegerType, true),
        DataTypes.createStructField("col5", DataTypes.DoubleType, true),
        DataTypes.createStructField("col6", DataTypes.BooleanType, true)
      )
    )
    // TotalColumns StructType
    val totalStructType =
      DataTypes.createStructType(Array.concat(primaryStructType.fields, attributeStructType.fields))

    // Register hive udf
    spark.sql("create temporary function ots_col_parser as " +
      "'org.apache.spark.sql.aliyun.udfs.tablestore.ResolveTableStoreBinlogUDF'")

    def genQuerySqlString(): String = {
      // Add Predefined columns
      val sb = new StringBuilder(
        "select __ots_record_type__ AS RecordType, __ots_record_timestamp__ AS RecordTimestamp," +
          " __ots_record_timestamp__ AS RecordId,"
      )
      // Add PrimaryKeys
      for (name <- primaryStructType.fieldNames) {
        sb.append(s" ${name},")
      }

      for (field <- attributeStructType.fields) {
        val name = field.name
        field.dataType match {
          case DataTypes.ShortType | DataTypes.IntegerType | DataTypes.LongType |
               DataTypes.ByteType =>
            sb.append(s" ots_col_parser(${name}, __ots_column_type_${name}) AS ${name},")
          case DataTypes.FloatType | DataTypes.DoubleType =>
            sb.append(s" ots_col_parser(${name}, __ots_column_type_${name}) AS ${name},")
          case DataTypes.BinaryType =>
            sb.append(s" ots_col_parser(${name}, __ots_column_type_${name}) AS ${name},")
          case DataTypes.BooleanType =>
            sb.append(s" ots_col_parser(${name}, __ots_column_type_${name}) AS ${name},")
          case DataTypes.StringType =>
            sb.append(s" ots_col_parser(${name}, __ots_column_type_${name}) AS ${name},")
        }
      }
      sb.deleteCharAt(sb.lastIndexOf(","))
      sb.append(" FROM stream")
      sb.toString
    }

    val sqlStatement = genQuerySqlString()
    log.info(s"Current query sql: $sqlStatement")

    val dataStream = spark.sql(sqlStatement)

    // Create empty schema with RDD
    val deltaTablePath = "/delta/streamrecords"
    try {
      DeltaTable.forPath(deltaTablePath)
    } catch {
      case e: AnalysisException if e.getMessage().contains("is not a Delta table") =>
        logWarning(s"${e.getMessage()}, now init the delta table with the empty dataframe.")
        log.info("initial delta table")
        val initialEmptyDf =
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], totalStructType)
        initialEmptyDf.write.format("delta").save(deltaTablePath)
    }

    val task = dataStream.writeStream
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .foreachBatch((ops: DataFrame, id: Long) => {
        val mergeDf = ops.select(
          col("RecordId"),
          col("RecordType"),
          col("RecordTimestamp"),
          col("PkString"),
          col("PkInt"),
          col("col1"),
          col("col2"),
          col("col3"),
          col("timestamp"),
          col("col5"),
          col("col6")
        )

        val primaryKeyNames = primaryStructType.names
        val mergeConf =
          s"""
             |target.${primaryKeyNames(0)} = source.${primaryKeyNames(0)}
             |AND target.${primaryKeyNames(1)} = source.${primaryKeyNames(1)}
             |""".stripMargin

        DeltaTable
          .forPath(spark, deltaTablePath)
          .as("target")
          .merge(mergeDf.as("source"), mergeConf)
          .whenMatched("source.RecordType='UPDATE' or source.RecordType='PUT'")
          .updateExpr(
            Map(
              "PkString" -> "source.PkString",
              "PkInt" -> "source.PkInt",
              "col1" -> "source.col1",
              "col2" -> "source.col2",
              "col3" -> "source.col3",
              "timestamp" -> "source.timestamp",
              "col5" -> "source.col5",
              "col6" -> "source.col6"
            )
          )
          .whenMatched("source.RecordType='DELETE'")
          .delete()
          .whenNotMatched("source.RecordType='PUT'")
          .insertExpr(
            Map(
              "PkString" -> "source.PkString",
              "PkInt" -> "source.PkInt",
              "col1" -> "source.col1",
              "col2" -> "source.col2",
              "col3" -> "source.col3",
              "timestamp" -> "source.timestamp",
              "col5" -> "source.col5",
              "col6" -> "source.col6"
            )
          )
          .execute()
      })
      .start()

    task.awaitTermination()
  }
}
