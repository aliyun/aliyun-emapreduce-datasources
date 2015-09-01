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

package org.apache.spark.aliyun.utils

import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.task.SQLTask
import com.aliyun.odps.{Partition, _}
import org.apache.spark.Logging

class OdpsUtils(odps: Odps) extends Logging{

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
    val partitionExist = if(partitionFilter.size == 0) false else true
    if(partitionExist) {
      (true, true)
    } else {
      (true, false)
    }
  }

  def dropPartition(
       project: String,
       table: String,
       pname: String): Boolean = {
    try {
      val (_, partitionE) = checkTableAndPartition(project, table, pname)
      if(!partitionE)
        return true
      odps.setDefaultProject(project)
      val partitionSpec = new PartitionSpec(pname)
      odps.tables().get(table).deletePartition(partitionSpec)
      true
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when delete partition " + pname + " of " + table + ".")
        logError(e.getMessage)
        return false
    }
  }

  def dropTable(
     project: String,
     table: String): Boolean = {
    try {
      val (tableE, _) = checkTableAndPartition(project, table, "random")
      if(!tableE)
        return true
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

  def createPartition(
     project: String,
     table: String,
     pname: String): Boolean = {
    val partitionSpec_ = new PartitionSpec(pname)
    val (tableE, partitionE) = checkTableAndPartition(project, table, pname)
    if(!tableE) {
      logWarning("table " + table + " do not exist, FAILED.")
      return false
    } else if(partitionE) {
      logWarning("table " + table + " partition " + pname + " exist, no need to create.")
      return true
    }

    try {
      odps.tables().get(table).createPartition(partitionSpec_)
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when create table " + table + " partition " + pname + ".")
        return false
    }

    true
  }


  def getTableSchema(project: String, table: String, isPartition: Boolean): Array[(String, String)] =  {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val columns = if (isPartition) schema.getPartitionColumns else schema.getColumns
    columns.toArray(new Array[Column](0)).map(e => {
      val name = e.getName
      val colType = e.getType match {
        case OdpsType.BIGINT => "BIGINT"
        case OdpsType.DOUBLE => "DOUBLE"
        case OdpsType.BOOLEAN => "BOOLEAN"
        case OdpsType.DATETIME => "DATETIME"
        case OdpsType.STRING => "STRING"
      }
      (name, colType)
    })
  }

  def getColumnByName(project: String, table: String, name: String): (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val idx = schema.getColumnIndex(name)
    val colType = schema.getColumn(name).getType match {
      case OdpsType.BIGINT => "BIGINT"
      case OdpsType.DOUBLE => "DOUBLE"
      case OdpsType.BOOLEAN => "BOOLEAN"
      case OdpsType.DATETIME => "DATETIME"
      case OdpsType.STRING => "STRING"
    }

    (idx.toString, colType)
  }

  def getColumnByIdx(project: String, table: String, idx: Int): (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val column = schema.getColumn(idx)
    val name = column.getName
    val colType = column.getType match {
      case OdpsType.BIGINT => "BIGINT"
      case OdpsType.DOUBLE => "DOUBLE"
      case OdpsType.BOOLEAN => "BOOLEAN"
      case OdpsType.DATETIME => "DATETIME"
      case OdpsType.STRING => "STRING"
    }

    (name, colType)
  }

  def runSQL(project: String, sqlCmd: String): Instance =  {
    val odps = new Odps(this.odps.getAccount)
    odps.setDefaultProject(project)
    log.info("SQL command: " + sqlCmd)
    try {
      SQLTask.run(odps, sqlCmd)
    } catch {
      case e: OdpsException => e.printStackTrace(); null
    }
  }

  def getAllPartitionSpecs(table: String, project: String = null): Iterator[PartitionSpec] = {
    if(project != null)
      odps.setDefaultProject(project)
    odps.tables().get(table).getPartitions.toArray(new Array[Partition](0))
      .map(pt => pt.getPartitionSpec).toIterator
  }
}

object OdpsUtils {
  def apply(accessKeyId: String, accessKeySecret: String, odpsUrl: String): OdpsUtils = {
    val account = new AliyunAccount(accessKeyId, accessKeySecret)
    val odps = new Odps(account)
    odps.setEndpoint(odpsUrl)
    new OdpsUtils(odps)
  }
}
