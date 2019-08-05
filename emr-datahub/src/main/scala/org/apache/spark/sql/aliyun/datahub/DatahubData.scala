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

import java.sql.Timestamp

import scala.collection.JavaConversions._

import com.alibaba.fastjson.JSONObject
import com.aliyun.datahub.common.data.FieldType
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

abstract class DatahubData(
    val project: String, val topic: String, val shardId: String, val systemTime: Timestamp)
  extends Serializable {
  def getContent: Array[Byte]
  def toArray: Array[Any]
}

class SchemaDatahubData(project: String, topic: String, shardId: String, systemTime: Timestamp,
    content: Array[(String, Any)])
  extends DatahubData(project, topic, shardId, systemTime)
    with Logging with Serializable {
  override def getContent: Array[Byte] = {
    val jo = new JSONObject()
    jo.put(DatahubSchema.PROJECT, project)
    jo.put(DatahubSchema.TOPIC, topic)
    jo.put(DatahubSchema.SHARD, shardId)
    jo.put(DatahubSchema.SYSTEM_TIME, systemTime)
    content.foreach(con => {
      jo.put(con._1, con._2)
    })
    jo.toString.getBytes()
  }

  override def toArray: Array[Any] = {
    Array(project, topic, shardId, systemTime) ++ content.map(_._2)
  }
}

class RawDatahubData(project: String, topic: String, shardId: String, systemTime: Timestamp, content: Array[Byte])
  extends DatahubData(project, topic, shardId, systemTime) {

  override def getContent: Array[Byte] = content

  override def toArray: Array[Any] = throw new UnsupportedOperationException
}

object DatahubSchema extends Logging {
  val PROJECT = "project"
  val TOPIC = "topic"
  val SHARD = "shardId"
  val SYSTEM_TIME = "systemTime"

  def isDefaultField(field: String): Boolean = {
    field == PROJECT || field == TOPIC || field == SHARD || field == SYSTEM_TIME
  }

  def getSchema(schema: Option[StructType], sourceOptions: Map[String, String]): StructType = {
    if (schema.isDefined && schema.get.nonEmpty) {
      schema.get
    } else {
      getSchema(sourceOptions)
    }
  }

  def getSchema(sourceOptions: Map[String, String]): StructType = {
    validateOptions(sourceOptions)
    val project = sourceOptions("project")
    val topic = sourceOptions("topic")
    val endpoint = sourceOptions("endpoint")
    val accessKeyId = sourceOptions("access.key.id")
    val accessKeySecret = sourceOptions("access.key.secret")

    try {
      getSchema(project, topic,
        accessKeyId, accessKeySecret, endpoint, sourceOptions)
    } catch {
      case e: Exception =>
        throw new Exception(s"Failed to analyse datahub schema", e)
    }
  }

  def getSchema(
      project: String,
      topic: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      sourceOptions: Map[String, String]): StructType = {
    var schema = new StructType()
    schema = schema.add(StructField(PROJECT, StringType, false))
    schema = schema.add(StructField(TOPIC, StringType, false))
    schema = schema.add(StructField(SHARD, StringType, false))
    schema = schema.add(StructField(SYSTEM_TIME, TimestampType, false))

    val client = DatahubOffsetReader.getOrCreateDatahubClient(accessKeyId, accessKeySecret, endpoint)
    client.getTopic(project, topic).getRecordSchema.getFields.foreach(field => {
      val struct = field.getType match {
        case FieldType.BIGINT => StructField(field.getName, DataTypes.LongType)
        case FieldType.BOOLEAN => StructField(field.getName, DataTypes.BooleanType)
        case FieldType.DECIMAL => {
          val precision = sourceOptions("decimal.precision").toInt
          val scale = sourceOptions("decimal.scale").toInt
          StructField(field.getName, DataTypes.createDecimalType(precision, scale))
        }
        case FieldType.DOUBLE => StructField(field.getName, DataTypes.DoubleType)
        case FieldType.TIMESTAMP => StructField(field.getName, DataTypes.LongType)
        case _ => StructField(field.getName, DataTypes.StringType)
      }
      schema = schema.add(struct)
    })
    schema
  }

  def validateOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    val project = caseInsensitiveParams.getOrElse("project",
      throw new MissingArgumentException("Missing datahub project (='project')."))
    val topic = caseInsensitiveParams.getOrElse("topic",
      throw new MissingArgumentException("Missing datahub topic (='topic')."))
    val accessKeyId = caseInsensitiveParams.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    val accessKeySecret = caseInsensitiveParams.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    val endpoint = caseInsensitiveParams.getOrElse("endpoint",
      throw new MissingArgumentException("Missing datahub endpoint (='endpoint')."))
    val client = DatahubOffsetReader.getOrCreateDatahubClient(accessKeyId, accessKeySecret, endpoint)
    val existDecimal = client.getTopic(project, topic).getRecordSchema.getFields.exists(x=> {
      x.getType == FieldType.DECIMAL
    })
    if (existDecimal) {
      val precision = caseInsensitiveParams.getOrElse("decimal.precision",
        throw new MissingArgumentException("Missing datahub decimal precision (='decimal.precision')." +
          " 'decimal.precision' must be set when there is decimal type in schema.")).toInt
      val scale = caseInsensitiveParams.getOrElse("decimal.scale",
        throw new MissingArgumentException("Missing datahub decimal precision (='decimal.scale'). " +
          "'decimal.scale' must be set when there is decimal type in schema.")).toInt
      if (precision > DecimalType.MAX_PRECISION || scale > DecimalType.MAX_SCALE) {
        throw new IllegalArgumentException(s"Option decimal.precision[${precision}] or decimal.scale[$scale] " +
          s"exceed max value in spark. Max precision is ${DecimalType.MAX_PRECISION }, " +
          s"max scale is ${DecimalType.MAX_SCALE}.")
      }
    }
  }
}