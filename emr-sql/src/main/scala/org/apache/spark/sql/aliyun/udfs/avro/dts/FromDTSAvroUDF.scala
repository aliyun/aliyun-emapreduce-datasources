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

import com.alibaba.dts.common.{FieldEntryHolder, Util}
import com.alibaba.dts.formats.avro.Field
import com.alibaba.dts.recordprocessor.{AvroDeserializer, FieldConverter}
import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BinaryObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.internal.Logging
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.collection.mutable

class FromDTSAvroUDF extends GenericUDTF with Logging {
  var _x1: BinaryObjectInspector = _

  private val OUT_COLS = 9

  @transient private val forwardColObj = Array.fill[java.lang.Object](OUT_COLS)(new java.lang.Object)
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
      } else throw new RuntimeException("invalid db and table name pair for record [" + record + "]")
    }

    val fields = record.getFields.asInstanceOf[ju.List[Field]].asScala
    val fieldArray = Util.getFieldEntryHolder(record)
    val beforeImages = fieldArray(0)
    val afterImages = fieldArray(1)

    forwardColObj(0) = record.getId
    forwardColObj(1) = record.getSource.toString
    forwardColObj(2) = s"$dbName.$tableName"
    forwardColObj(3) = record.getOperation.toString
    forwardColObj(4) = new java.sql.Timestamp(record.getSourceTimestamp * 1000L)
    forwardColObj(5) = record.getTags.isEmpty match {
      case true => "{}"
      case false => compact(render(record.getTags.asScala.map(i => i._1 -> JString(i._2) : JObject).reduce(_ ~ _)))
    }
    forwardColObj(6) = compact(render(JArray(fields.map(_.getName).map(JString(_)).toList)))
    forwardColObj(7) = makeImageString(beforeImages, fields)
    forwardColObj(8) = makeImageString(afterImages, fields)

    forward(forwardColObj)
  }

  private def makeImageString(holder: FieldEntryHolder, fields: Seq[Field]): String = {
    val imageMap = new mutable.HashMap[String, String]()
    fields.foreach(field => {
      val item = holder.take()
      if (item != null) {
        val image = FIELD_CONVERTER.convert(field, item).toString
        imageMap.put(field.getName, image)
      }
    })
    imageMap.isEmpty match {
      case true => "{}"
      case false => compact(render(imageMap.map(i => i._1 -> JString(i._2) : JObject).reduce(_ ~ _)))
    }
  }
}
