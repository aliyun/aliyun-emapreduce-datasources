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

import com.alibaba.fastjson.JSONObject
import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class DatahubRecordToUnsafeRowConverter(
    schema: StructType,
    sourceOptions: Map[String, String]) {
  private val fallback = schema.sameType(DatahubSchema.getDefaultSchema)
  private val rowWriter = new UnsafeRowWriter(schema.fields.length)
  private val precision = sourceOptions("decimal.precision").toInt
  private val scale = sourceOptions("decimal.scale").toInt

  def toUnsafeRow(
      record: RecordEntry,
      project: String,
      topic: String,
      shardId: String): UnsafeRow = {
    rowWriter.reset()
    rowWriter.zeroOutNullBytes()
    if (!fallback) {
      rowWriter.write(0, UTF8String.fromString(project))
      rowWriter.write(1, UTF8String.fromString(topic))
      rowWriter.write(2, UTF8String.fromString(shardId))
      rowWriter.write(3, DateTimeUtils.fromJavaTimestamp(
        new java.sql.Timestamp(record.getSystemTime)))

      var idx = 4
      schema.fields.filter(f => !DatahubSchema.isDefaultField(f.name))
        .foreach(field => {
          field.dataType match {
            case LongType => rowWriter.write(idx, record.getBigint(field.name))
            case BooleanType => rowWriter.write(idx, record.getBoolean(field.name))
            case _: DecimalType =>
              val v = record.getDecimal(field.name)
              rowWriter.write(idx, Decimal(v, precision, scale), precision, scale)
            case DoubleType => rowWriter.write(idx, record.getDouble(field.name))
            case TimestampType => rowWriter.write(idx,
              DateTimeUtils.fromJavaTimestamp(
                new java.sql.Timestamp(record.getTimeStampAsMs(field.name))))
            case _ => rowWriter.write(idx, UTF8String.fromString(record.getString(field.name)))
        }
        idx += 1
      })
    } else {
      rowWriter.write(0, UTF8String.fromString(project))
      rowWriter.write(1, UTF8String.fromString(topic))
      rowWriter.write(2, UTF8String.fromString(shardId))
      rowWriter.write(3, DateTimeUtils.fromJavaTimestamp(
        new java.sql.Timestamp(record.getSystemTime)))

      val obj = new JSONObject()
      record.getFields.foreach(field => {
        obj.put(field.getName, record.get(field.getName))
      })

      rowWriter.write(4, obj.toJSONString.getBytes)
    }

    rowWriter.getRow
  }
}
