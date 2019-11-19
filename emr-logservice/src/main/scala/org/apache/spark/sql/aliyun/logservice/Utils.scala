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

import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.Base64

import com.alibaba.fastjson.{JSON, JSONObject}
import com.aliyun.openservices.log.common.{LogContent, LogItem}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Utils extends Logging {

  private type ValueConverter = String => Any

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
        throw new UnsupportedOperationException(
          s"Unsupported type $dataType when sink to log store.")
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
        throw new UnsupportedOperationException(
          s"Unsupported type $dataType when sink to log store.")
      case MapType(StringType, _, _) =>
        throw new UnsupportedOperationException(
          s"Unsupported type $dataType when sink to log store.")
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
              val logContent =
                new LogContent(fieldNamesIterator.next(), converter(rowIterator.next()).toString)
              record.PushBack(logContent)
            }
            record
          }
        }
    }
  }

  def makeConverter(
      name: String,
      dataType: DataType,
      nullable: Boolean = true): ValueConverter = dataType match {
    case _: ByteType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toByte)

    case _: ShortType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toShort)

    case _: IntegerType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toInt)

    case _: LongType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toLong)

    case _: FloatType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toFloat)

    case _: DoubleType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toDouble)

    case _: BooleanType => (d: String) =>
      nullSafeDatum(d, name, nullable)(_.toBoolean)

    case dt: DecimalType => (d: String) =>
      nullSafeDatum(d, name, nullable) { datum =>
        val value = new BigDecimal(datum.replaceAll(",", ""))
        Decimal(value, dt.precision, dt.scale)
      }

    case _: TimestampType => (d: String) =>
      nullSafeDatum(d, name, nullable) { datum =>
        DateTimeUtils.stringToTime(datum).getTime * 1000L
      }

    case _: DateType => (d: String) =>
      nullSafeDatum(d, name, nullable) { datum =>
        DateTimeUtils.millisToDays(DateTimeUtils.stringToTime(datum).getTime)
      }

    case _: StringType => (d: String) =>
      nullSafeDatum(d, name, nullable)(UTF8String.fromString)

    case udt: UserDefinedType[_] => (datum: String) =>
      makeConverter(name, udt.sqlType, nullable)

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
  }

  private def nullSafeDatum(
     datum: String,
     name: String,
     nullable: Boolean)(converter: ValueConverter): Any = {
    if (datum == null) {
      if (!nullable) {
        throw new RuntimeException(s"null value found but field $name is not nullable.")
      }
      null
    } else {
      converter.apply(datum)
    }
  }

  def createSpecificOffset(
      logProject: String,
      logStore: String,
      offset: Int,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      ignoreError: Boolean = true): String = {
    val caseInsensitiveParams = Map(
      "sls.project" -> logProject,
      "sls.store" -> logStore,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "endpoint" -> endpoint
    )
    val loghubOffsetReader = new LoghubOffsetReader(caseInsensitiveParams)
    val earliestOffsets = loghubOffsetReader.fetchEarliestOffsets()
    val endOffsets = loghubOffsetReader.fetchLatestOffsets()
    require(earliestOffsets.size == endOffsets.size,
      s"""The size of earliestOffsets dose not equals size of endOffsets
        | earliestOffsets: $earliestOffsets
        |
        | endOffsets: $endOffsets
      """.stripMargin)
    val startOffsets = earliestOffsets.toSeq.sortBy(_._1.shard)
      .zip(endOffsets.toSeq.sortBy(_._1.shard))
      .map { case (startOffset, endOffset) =>
        require(startOffset._1.shard == endOffset._1.shard)
        if (offset < startOffset._2._1) {
          val msg = s"Specific offset [$offset] is less than shard ${startOffset._1.shard} start " +
            s"offset [${startOffset._2}], using [${startOffset._2}] instead of [$offset] as new " +
            "specific offset."
          if (ignoreError) {
            logWarning(msg)
            startOffset
          } else {
            throw new Exception(msg)
          }
        } else if (offset > endOffset._2._1) {
          val msg = s"Specific offset [$offset] is larger than shard ${endOffset._1.shard} end " +
            s"offset [${endOffset._2}], using [${endOffset._2}] instead of [$offset] as new " +
            "specific offset."
          if (ignoreError) {
            logWarning(msg)
            endOffset
          } else {
            throw new Exception(msg)
          }
        } else {
          (startOffset._1, (offset, ""))
        }
      }.toMap
    LoghubSourceOffset.partitionOffsets(startOffsets)
  }

  def decodeCursorToTimestamp(cursor: String): Long = {
    val timestampAsBytes = Base64.getDecoder.decode(cursor.getBytes(StandardCharsets.UTF_8))
    val timestamp = new String(timestampAsBytes, StandardCharsets.UTF_8)
    timestamp.toLong
  }

  /**
   * Used to update loghub source config.
   */
  def updateSourceConfig(
      zkConnect: String,
      checkpoint: String,
      logProject: String,
      logStore: String,
      sourceProps: Map[String, String]): Unit = {
    val zkConnectTimeoutMs = 10000
    val zkSessionTimeoutMs = 10000
    val zkConnection = new ZkConnection(zkConnect, zkSessionTimeoutMs)
    val zkClient = new ZkClient(zkConnection, zkConnectTimeoutMs, new ZKStringSerializer())
    try {
      val configPathParent = if (checkpoint.endsWith("/")) {
        s"${checkpoint}sources"
      } else {
        s"$checkpoint/sources"
      }
      val configPath = s"$configPathParent/config"
      val configFileExists = zkClient.exists(configPath)
      if (configFileExists) {
        val data: String = zkClient.readData(configPath)
        val jsonObject = JSON.parseObject(data)
        val version = jsonObject.getString("version")
        if ("v1".equals(version)) {
          val configObject = jsonObject.getJSONObject("config")
          if (configObject.containsKey(logProject)) {
            val logProjectObject = configObject.getJSONObject(logProject)
            if (logProjectObject.containsKey(logStore)) {
              val logStoreObject = logProjectObject.getJSONObject(logStore)
              sourceProps.foreach(kv => {
                logStoreObject.put(kv._1, kv._2)
              })
            } else {
              val logStoreObject = new JSONObject()
              sourceProps.foreach(kv => {
                logStoreObject.put(kv._1, kv._2)
              })
              logProjectObject.put(logStore, logStoreObject)
            }
          } else {
            val logProjectObject = new JSONObject()
            val logStoreObject = new JSONObject()
            sourceProps.foreach(kv => {
              logStoreObject.put(kv._1, kv._2)
            })
            logProjectObject.put(logStore, logStoreObject)
            configObject.put(logProject, logProjectObject)
          }
          zkClient.writeData(configPath, jsonObject.toJSONString)
        } else {
          throw new Exception(
            s"""Unsupported dynamic config data version $version, only support ["v1"]""")
        }
      } else {
        val jsonObject = new JSONObject()
        val configObject = new JSONObject()
        val logProjectObject = new JSONObject()
        val logStoreObject = new JSONObject()
        sourceProps.foreach(kv => {
          logStoreObject.put(kv._1, kv._2)
        })
        logProjectObject.put(logStore, logStoreObject)
        configObject.put(logProject, logProjectObject)
        jsonObject.put("config", configObject)
        jsonObject.put("version", "v1")
        zkClient.createPersistent(configPathParent, true)
        zkClient.createPersistent(configPath, jsonObject.toJSONString)
      }
    } catch {
      case e: Exception =>
        throw new Exception(s"Failed to update config for [$logProject/$logStore]", e)
    } finally {
      zkClient.close()
    }
  }

  /**
   * Used to update loghub source config.
   */
  def updateSourceConfig(
      zkConnect: String,
      checkpoint: String,
      logProject: String,
      logStore: String,
      key: String,
      value: String): Unit = {
    updateSourceConfig(zkConnect, checkpoint, logProject, logStore, Map(key -> value))
  }
}
