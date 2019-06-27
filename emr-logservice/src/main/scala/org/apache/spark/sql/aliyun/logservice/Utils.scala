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

package org.apache.spark.sql.aliyun.logservice

import java.sql.{Date, Timestamp}

import com.aliyun.openservices.log.common.{LogContent, LogItem}
import com.aliyun.openservices.log.exception.LogException
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Utils extends Logging {
  def getSchema(schema: Option[StructType], sourceOptions: Map[String, String]): StructType = {
    validateOptions(sourceOptions)
    val logProject = sourceOptions("sls.project")
    val logStore = sourceOptions("sls.store")
    val endpoint = sourceOptions("endpoint")
    val accessKeyId = sourceOptions("access.key.id")
    val accessKeySecret = sourceOptions("access.key.secret")
    if (schema.isDefined && schema.get.nonEmpty) {
      schema.get
    } else {
      try {
        LoghubOffsetReader.loghubSchema(logProject, logStore,
          accessKeyId, accessKeySecret, endpoint)
      } catch {
        case e: LogException =>
          logWarning(s"Failed to analyse loghub schema, fall back to default " +
            s"schema ${LoghubOffsetReader.loghubSchema}", e)
          LoghubOffsetReader.loghubSchema
      }
    }
  }

  def validateOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    caseInsensitiveParams.getOrElse("sls.project",
      throw new MissingArgumentException("Missing logService project (='sls.project')."))
    caseInsensitiveParams.getOrElse("sls.store",
      throw new MissingArgumentException("Missing logService store (='sls.store')."))
    caseInsensitiveParams.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    caseInsensitiveParams.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    caseInsensitiveParams.getOrElse("endpoint",
      throw new MissingArgumentException("Missing log store endpoint (='endpoint')."))
  }

  def toConverter(dataType: DataType): (Any) => Any = {
    dataType match {
      case BinaryType =>
        throw new UnsupportedOperationException(s"Unsupported type $dataType when sink to log store.")
      case ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType | StringType | BooleanType => identity
      case d: DecimalType =>
        (item: Any) =>
          if (item == null) {
            null
          } else {
            val data = Decimal(item.asInstanceOf[java.math.BigDecimal], d.precision, d.scale)
            data.toDouble
          }
      case TimestampType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Timestamp].getTime
      case DateType => (item: Any) =>
        if (item == null) null else item.asInstanceOf[Date].getTime
      case ArrayType(_, _) =>
        throw new UnsupportedOperationException(s"Unsupported type $dataType when sink to log store.")
      case MapType(StringType, _, _) =>
        throw new UnsupportedOperationException(s"Unsupported type $dataType when sink to log store.")
      case structType: StructType =>
        val fieldConverters = structType.fields.map(field => toConverter(field.dataType))
        (item: Any) => {
          if (item == null) {
            null
          } else {
            val record = new LogItem()
            val convertersIterator = fieldConverters.iterator
            val fieldNamesIterator = dataType.asInstanceOf[StructType].fieldNames.iterator
            val rowIterator = item.asInstanceOf[Row].toSeq.iterator

            while (convertersIterator.hasNext) {
              val converter = convertersIterator.next()
              val logContent = new LogContent(fieldNamesIterator.next(), converter(rowIterator.next()).toString)
              record.PushBack(logContent)
            }
            record
          }
        }
    }
  }
}
