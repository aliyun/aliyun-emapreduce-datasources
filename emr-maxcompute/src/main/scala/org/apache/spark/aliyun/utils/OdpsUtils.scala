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
package org.apache.spark.aliyun.utils

import java.sql.SQLException

import com.aliyun.odps.{Partition, _}
import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.task.SQLTask

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

class OdpsUtils(odps: Odps) extends Logging{

  /**
   * Check if specific ODPS table and partition exist or else.
   *
   * <h4>Examples</h4>
   * <blockquote>
   * <table border=0 cellspacing=3 cellpadding=0 summary="Examples of checking
    * ODPS table and partition existence">
   *     <tr bgcolor="#ccccff">
   *         <th align=left>Type of ODPS table
   *         <th align=left>Table exist
   *         <th align=left>Partition exist
   *         <th align=left>Return
   *     <tr>
   *         <td><code>Non-partitioned</code>
   *         <td><code>false</code>
   *         <td><code>-</code>
   *         <td><code>(false, false)</code>
   *     <tr bgcolor="#eeeeff">
   *         <td><code>Non-partitioned</code>
   *         <td><code>true</code>
   *         <td><code>-</code>
   *         <td><code>(true, false)</code>
   *     <tr>
   *         <td><code>Partitioned</code>
   *         <td><code>true</code>
   *         <td><code>false</code>
   *         <td><code>(true, false)</code>
   *     <tr bgcolor="#eeeeff">
   *         <td><code>Partitioned</code>
   *         <td><code>true</code>
   *         <td><code>true</code>
   *         <td><code>(true, true)</code>
   *     <tr>
   *         <td><code>Partitioned</code>
   *         <td><code>false</code>
   *         <td><code>-</code>
   *         <td><code>(false, false)</code>
   * </table>
   * </blockquote>
   *
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param pname The name of ODPS table partition, if partitioned table.
   */
  def checkTableAndPartition(
      project: String,
      table: String,
      pname: String): (Boolean, Boolean) = {
    val partitionSpec_ = new PartitionSpec(pname)
    odps.setDefaultProject(project)
    val tables = odps.tables()
    val tableExist = tables.exists(table)
    if(!tableExist) {
      logWarning("table " + table + " do not exist!")
      return (false, false)
    }

    val partitions = tables.get(table).getPartitions
    val partitionFilter = partitions.toArray(new Array[Partition](0)).iterator
      .map(e => e.getPartitionSpec)
      .filter(f => f.toString.equals(partitionSpec_.toString))
    val partitionExist = if (partitionFilter.size == 0) false else true
    if(partitionExist) {
      (true, true)
    } else {
      (true, false)
    }
  }

  /**
   * Drop specific partition of ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param pname The name of ODPS table partition, if partitioned table.
   * @return Success or not.
   */
  def dropPartition(
      project: String,
      table: String,
      pname: String): Boolean = {
    try {
      val (_, partitionE) = checkTableAndPartition(project, table, pname)
      if(!partitionE) {
        return true
      }
      odps.setDefaultProject(project)
      val partitionSpec = new PartitionSpec(pname)
      odps.tables().get(table).deletePartition(partitionSpec)
      true
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when delete partition " + pname +
          " of " + table + ".")
        logError(e.getMessage)
        return false
    }
  }

  /**
   * Drop specific ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return Success or not.
   */
  def dropTable(
      project: String,
      table: String): Boolean = {
    try {
      val (tableE, _) = checkTableAndPartition(project, table, "random")
      if(!tableE) {
        return true
      }
      odps.setDefaultProject(project)
      odps.tables().delete(table)
      true
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when delete table " + table + ".")
        logError(e.getMessage)
        return false
    }
  }

  /**
   * Create ODPS table.
   *
   * @param project Name of odps project.
   * @param table Name of odps table.
   * @param schema refer to {{TableSchema}}.
   * @param ifNotExists Fail or not if target table exists.
   */
  def createTable(
      project: String,
      table: String,
      schema: TableSchema,
      ifNotExists: Boolean): Unit = {
    odps.setDefaultProject(project)
    odps.tables().create(table, schema, ifNotExists)
  }

  /**
   * Create specific partition of ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param pname The name of ODPS table partition, if partitioned table.
   * @return Success or not.
   */
  def createPartition(
      project: String,
      table: String,
      pname: String): Boolean = {
    val partitionSpec_ = new PartitionSpec(pname)
    val (tableE, partitionE) = checkTableAndPartition(project, table, pname)
    if(!tableE) {
      logWarning("table " + table + " do not exist, FAILED.")
      return false
    } else if (partitionE) {
      logWarning("table " + table + " partition " + pname + " exist, " +
        "no need to create.")
      return true
    }

    try {
      odps.tables().get(table).createPartition(partitionSpec_)
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when create table " + table +
          " partition " + pname + ".")
        return false
    }

    true
  }

  /**
   * Get the table schema of ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param isPartition Is partition column or not.
   * @return
   */
  def getTableSchema(project: String, table: String, isPartition: Boolean):
      Array[(String, TypeInfo)] = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val columns = if (isPartition) schema.getPartitionColumns else schema.getColumns
    columns.toArray(new Array[Column](0)).map(e => (e.getName, e.getTypeInfo))
  }

  /**
   * Get information of specific column via column name.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param name The name of specific column.
   * @return Column index and type.
   */
  def getColumnByName(project: String, table: String, name: String):
      (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val idx = schema.getColumnIndex(name)
    val colType = schema.getColumn(name).getTypeInfo
    val field = getCatalystType(name, colType, true)

    (idx.toString, field.dataType.simpleString)
  }

  /**
   * Get information of specific column via column index.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param idx The index of specific column.
   * @return Column name and type.
   */
  def getColumnByIdx(project: String, table: String, idx: Int):
      (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val column = schema.getColumn(idx)
    val name = column.getName
    val colType = schema.getColumn(name).getTypeInfo
    val field = getCatalystType(name, colType, true)

    (name, field.dataType.simpleString)
  }

  /**
   * Run sql on ODPS.
   *
   * @param project The name of ODPS project.
   * @param sqlCmd An ODPS sql
   * @return An instance of ODPS.
   */
  def runSQL(project: String, sqlCmd: String, hints: Map[String, String] = Map.empty): Instance = {
    odps.setDefaultProject(project)
    log.info("SQL command: " + sqlCmd)
    try {
      import scala.collection.JavaConverters._
      SQLTask.run(odps, project, sqlCmd, hints.asJava, null)
    } catch {
      case e: OdpsException => e.printStackTrace(); null
    }
  }

  /**
   * Get all partition [[PartitionSpec]] of specific ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return All partition [[PartitionSpec]]
   */
  def getAllPartitionSpecs(table: String, project: String = null):
      Iterator[PartitionSpec] = {
    if (project != null) {
      odps.setDefaultProject(project)
    }
    odps.tables().get(table).getPartitions.toArray(new Array[Partition](0))
      .map(pt => pt.getPartitionSpec).toIterator
  }

  /**
   * Check if the table is a partition table
   *
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return
   */
  def isPartitionTable(table: String, project: String = null): Boolean = {
    if (project != null) {
      odps.setDefaultProject(project)
    }
    odps.tables().get(table).isPartitioned
  }

  /**
   * Check if the table exists
   *
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return
   */
  def tableExist(table: String, project: String = null): Boolean = {
    if (project != null) {
      odps.setDefaultProject(project)
    }
    odps.tables().exists(table)
  }

  /**
   * Check if the partition exists in the table,
   *
   * `partitionSpec` like `pt='xxx',ds='yyy'`
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return
   */
  def partitionExist(partitionSpec: String, table: String, project: String = null): Boolean = {
    if (project != null) {
      odps.setDefaultProject(project)
    }
    val partitions = odps.tables().get(table).getPartitions
    val partitionFilter = partitions.toArray(new Array[Partition](0)).iterator
      .map(e => e.getPartitionSpec)
      .filter(f => f.toString.equals(partitionSpec.toString))

    if (partitionFilter.size == 0) false else true
  }

  def getCatalystType(columnName: String, columnType: TypeInfo, nullable: Boolean): StructField = {
    val metadata = new MetadataBuilder()
      .putString("name", columnName)
      .putLong("scale", 0L)

    val answer = columnType.getOdpsType match {
      case OdpsType.BIGINT => LongType
      case OdpsType.BINARY => BinaryType
      case OdpsType.BOOLEAN => BooleanType
      case OdpsType.CHAR => StringType
      case OdpsType.DATE => DateType
      case OdpsType.DATETIME => DateType
      case OdpsType.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case OdpsType.DOUBLE => DoubleType
      case OdpsType.FLOAT => FloatType
      case OdpsType.INT => IntegerType
      case OdpsType.SMALLINT => ShortType
      case OdpsType.STRING => StringType
      case OdpsType.TINYINT => ByteType
      case OdpsType.VARCHAR => StringType
      case OdpsType.TIMESTAMP => TimestampType
      case OdpsType.VOID => NullType
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
      case _ => throw new SQLException(s"Unsupported type $columnType")
    }

    StructField(columnName, answer, nullable, metadata.build())
  }
}

object OdpsUtils {
  def apply(accessKeyId: String, accessKeySecret: String, odpsUrl: String):
      OdpsUtils = {
    val account = new AliyunAccount(accessKeyId, accessKeySecret)
    val odps = new Odps(account)
    odps.setEndpoint(odpsUrl)
    new OdpsUtils(odps)
  }
}
