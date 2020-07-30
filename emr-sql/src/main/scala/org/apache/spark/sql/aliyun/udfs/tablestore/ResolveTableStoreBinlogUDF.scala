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

package org.apache.spark.sql.aliyun.udfs.tablestore

import org.apache.hadoop.hive.ql.exec.{Description, UDF}

import org.apache.spark.internal.Logging

@Description(
  name = ResolveTableStoreBinlogUDF.name,
  value = ResolveTableStoreBinlogUDF.value,
  extended = ResolveTableStoreBinlogUDF.extendedValue
)
class ResolveTableStoreBinlogUDF extends UDF with Logging {
  def evaluate(col: Long, columnType: String): Long = {
    ResolveTableStoreBinlogUDF.getActualValue[Long](col, columnType)
  }

  def evaluate(col: String, columnType: String): String = {
    ResolveTableStoreBinlogUDF.getActualValue[String](col, columnType)
  }

  def evaluate(col: Double, columnType: String): Double = {
    ResolveTableStoreBinlogUDF.getActualValue[Double](col, columnType)
  }

  def evaluate(col: Boolean, columnType: String): Boolean = {
    ResolveTableStoreBinlogUDF.getActualValue[Boolean](col, columnType)
  }

  def evaluate(col: Array[Byte], columnType: String): Array[Byte] = {
    ResolveTableStoreBinlogUDF.getActualValue[Array[Byte]](col, columnType)
  }
}

object ResolveTableStoreBinlogUDF {
  final val name = "ots_col_parser"
  final val value = "_FUNC_(<ColumnName>, __ots_column_type_<ColumnName>) " +
    "- return the actual column value of tablestore binlog."
  final val extendedValue =
    // scalastyle:off
    """
      |The change data from tablestore has two parts: Predefined columns and user defined columns.
      | Predefined Columns:
      |   __ots_record_type__ (STRING): The record type of the change data, valid in (PUT, UPDATE, DELETE).
      |   __ots_record_timestamp__ (LONG): The record timestamp of the change data, in nanosecond.
      |   __ots_column_type_<ColumnName> (STRING):
      |      The operation of the column in change data, valid in (PUT, DELETE_ONE_VERSION, DELETE_ALL_VERSION).
      |
      | User defined columns:
      |   The user defined schema in DataStreamReader(in option catalog).
      |
      | Example:
      |   Suppose user defined 7 columns: PkString, PkInt, col_long, col_string, col_binary, col_double, col_boolean
      |   > select __ots_record_type__ AS RecordType, __ots_record_timestamp__ AS RecordTimestamp, PkString, PkInt,
      |       ots_col_parser(col_string, __ots_column_type_col_string) AS col_string,
      |       ots_col_parser(col_long, __ots_column_type_col_long) AS col_long,
      |       ots_col_parser(col_binary, __ots_column_type_col_binary) AS col_binary,
      |       ots_col_parser(col_double, __ots_column_type_col_double) AS col_double,
      |       ots_col_parser(col_boolean, __ots_column_type_col_boolean) AS col_boolean FROM stream_view;
      |   PUT 1595990621936075 00008 1595990621 str1 123456 null 3.1415 true
      |
    """
    // scalastyle:on

  // Get actual value according column operation type.
  def getActualValue[T](origValue: T, columnType: String): T = {
    columnType match {
      case "PUT" => origValue
      case "DELETE_ONE_VERSION" => null.asInstanceOf[T]
      case "DELETE_ALL_VERSION" => null.asInstanceOf[T]
      case _ => null.asInstanceOf[T]
    }
  }
}

