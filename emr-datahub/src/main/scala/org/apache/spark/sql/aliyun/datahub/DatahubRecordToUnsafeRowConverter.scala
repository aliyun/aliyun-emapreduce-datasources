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

package org.apache.spark.sql.aliyun.datahub

import com.aliyun.datahub.model.RecordEntry

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DatahubRecordToUnsafeRowConverter(
    schema: StructType,
    sourceOptions: Map[String, String]) {
  private val rowWriter = new UnsafeRowWriter(schema.fields.length)
  private lazy val precision = sourceOptions("decimal.precision").toInt
  private lazy val scale = sourceOptions("decimal.scale").toInt

  def toUnsafeRow(
      record: RecordEntry,
      project: String,
      topic: String,
      shardId: String): UnsafeRow = {
    rowWriter.reset()
    rowWriter.zeroOutNullBytes()

    var idx = 0
    schema.fields.foreach(field => {
      field.dataType match {
        case LongType =>
          val value = record.getBigint(field.name)
          if (value != null) {
            rowWriter.write(idx, value)
          } else {
            rowWriter.setNullAt(idx)
          }
        case BooleanType =>
          val value = record.getBoolean(field.name)
          if (value != null) {
            rowWriter.write(idx, value)
          } else {
            rowWriter.setNullAt(idx)
          }
        case _: DecimalType =>
          val value = record.getDecimal(field.name)
          if (value != null) {
            rowWriter.write(idx, Decimal(value, precision, scale), precision, scale)
          } else {
            rowWriter.setNullAt(idx)
          }
        case DoubleType =>
          val value = record.getDouble(field.name)
          if (value != null) {
            rowWriter.write(idx, value)
          } else {
            rowWriter.setNullAt(idx)
          }
        case TimestampType =>
          val value = record.getTimeStampAsMs(field.name)
          if (value != null) {
            rowWriter.write(idx,
              DateTimeUtils.fromJavaTimestamp(new java.sql.Timestamp(value)))
          } else {
            rowWriter.setNullAt(idx)
          }
        case _ =>
          val value = record.getString(field.name)
          if (value != null) {
            rowWriter.write(idx, UTF8String.fromString(value))
          } else {
            rowWriter.setNullAt(idx)
          }
      }
      idx += 1
    })

    rowWriter.getRow
  }
}
