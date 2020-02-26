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

package org.apache.spark.sql.aliyun.udfs.avro.dts

import java.{util => ju}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.alibaba.dts.common.{FieldEntryHolder, Util}
import com.alibaba.dts.formats.avro.Field
import com.alibaba.dts.recordprocessor.{AvroDeserializer, FieldConverter}
import org.apache.hadoop.hive.ql.exec.{Description, UDFArgumentException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BinaryObjectInspector, PrimitiveObjectInspectorFactory}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging

@Description(
  name = FromDTSAvroUDF.name,
  value = FromDTSAvroUDF.value,
  extended = FromDTSAvroUDF.extendedValue)
class FromDTSAvroUDF extends GenericUDTF with Logging {
  var _x1: BinaryObjectInspector = _

  private val OUT_COLS = 9

  @transient private val forwardColObj =
    Array.fill[java.lang.Object](OUT_COLS)(new java.lang.Object)
  @transient private var inputOIs: Array[ObjectInspector] = null
  @transient private val fastDeserializer = new AvroDeserializer()
  @transient private val FIELD_CONVERTER = FieldConverter.getConverter("mysql", null)

  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
    if (argOIs.length != 1) {
      throw new UDFArgumentException(
        s"""from_avro requires 1 arguments, got ${argOIs.length}.
           |Arguments should be: (value).
           |
           |            value: binary data in avro format
         """.stripMargin)
    }

    val Array(x1) = argOIs

    if (!x1.isInstanceOf[BinaryObjectInspector]) {
      val errorMsg =
        s"""Argument type error.
           |(value: binary)
           |(${x1.isInstanceOf[BinaryObjectInspector]})
        """.stripMargin
      logError(errorMsg)
      throw new UDFArgumentException(errorMsg)
    }

    inputOIs = argOIs
    val outFieldNames = new ju.ArrayList[String]
    val outFieldOIs = new ju.ArrayList[ObjectInspector]
    outFieldNames.add("recordID")
    outFieldNames.add("source")
    outFieldNames.add("dbTable")
    outFieldNames.add("recordType")
    outFieldNames.add("recordTimestamp")
    outFieldNames.add("extraTags")
    outFieldNames.add("fields")
    outFieldNames.add("beforeImages")
    outFieldNames.add("afterImages")
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaTimestampObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    outFieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    ObjectInspectorFactory.getStandardStructObjectInspector(outFieldNames, outFieldOIs)
  }

  override def close(): Unit = {}

  override def process(args: Array[AnyRef]): Unit = {
    val input = inputOIs(0).asInstanceOf[BinaryObjectInspector].getPrimitiveJavaObject(args(0))
    val record = fastDeserializer.deserialize(input)
    var dbName: String = null
    var tableName: String = null
    val dbPair = Util.uncompressionObjectName(record.getObjectName)
    if (null != dbPair) {
      if (dbPair.length == 2) {
        dbName = dbPair(0)
        tableName = dbPair(1)
      } else if (dbPair.length == 3) {
        dbName = dbPair(0)
        tableName = dbPair(2)
      } else if (dbPair.length == 1) {
        dbName = dbPair(0)
        tableName = ""
      } else {
        throw new RuntimeException("invalid db and table name pair for record [" + record + "]")
      }
    }

    val fields = Option(record.getFields).map(_.asInstanceOf[ju.List[Field]].asScala)
    val fieldArray = Util.getFieldEntryHolder(record)
    val beforeImages = fieldArray(0)
    val afterImages = fieldArray(1)

    forwardColObj(0) = record.getId
    forwardColObj(1) = record.getSource.toString
    forwardColObj(2) = s"$dbName.$tableName"
    forwardColObj(3) = record.getOperation.toString
    forwardColObj(4) = new java.sql.Timestamp(record.getSourceTimestamp * 1000L)
    forwardColObj(5) = record.getTags.isEmpty match {
      case true => null
      case false => compact(
        render(record.getTags.asScala.map(i => i._1 -> JString(i._2) : JObject).reduce(_ ~ _)))
    }
    forwardColObj(6) = fields.map { f =>
      compact(render(JArray(f.map(_.getName).map(JString(_)).toList)))
    }.orNull
    forwardColObj(7) = makeImageString(beforeImages, fields)
    forwardColObj(8) = makeImageString(afterImages, fields)

    forward(forwardColObj)
  }

  private def makeImageString(holder: FieldEntryHolder, fields: Option[Seq[Field]]): String = {
    try {
      if (holder == null) {
        return null
      }
      val imageMap = new mutable.HashMap[String, String]()
      fields.map { f =>
        f.foreach(field => {
          val item = holder.take()
          if (item != null) {
            val image = FIELD_CONVERTER.convert(field, item).toString
            imageMap.put(field.getName, image)
          }
        })
        imageMap.isEmpty match {
          case true => null
          case false => compact(
            render(imageMap.map(i => i._1 -> JString(i._2): JObject).reduce(_ ~ _)))
        }
      }.orNull
    } catch {
      case e: Exception =>
        throw new Exception("Failed to make image string.", e)
    }
  }
}

object FromDTSAvroUDF {
  final val name = "dts_binlog_parser"
  final val value = "_FUNC_(binary) - Returns a struct value with the given `binary` value."
  final val extendedValue =
    // scalastyle:off
    """
      |Only binary data from Aliyun DTS is supported.
      |Returned struct value schema:
      | | recordID long
      | | source string
      | | dbTable string
      | | recordType string
      | | recordTimestamp timestamp
      | | extraTags string
      | | fields string
      | | beforeImages string
      | | afterImages string
      |
      |Example:
      | > SELECT dts_binlog_parser(binary)
      |   10	{"sourceType": "MySQL", "version": "5.7.26-log"}	students	UPDATE	2020-01-10 17:23:18	{"pk_uk_info":"{}","readerThroughoutTime":"1578648243764"}	["id","name","__#alibaba_rds_row_id#__"]	{"__#alibaba_rds_row_id#__":"44","name":"jack","id":"1"}	{"__#alibaba_rds_row_id#__":"44","name":"jack","id":"2"}
    """
    // scalastyle:on
}
