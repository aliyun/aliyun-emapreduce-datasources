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

  // TODO: remove default schema
  def getDefaultSchema: StructType = {
    StructType(Seq(
      StructField(PROJECT, StringType),
      StructField(TOPIC, StringType),
      StructField(SHARD, StringType),
      StructField(SYSTEM_TIME, TimestampType),
      StructField("value", BinaryType)
    ))
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
        accessKeyId, accessKeySecret, endpoint)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to analyse datahub schema, fall back to default " +
          s"schema ${getDefaultSchema}", e)
        getDefaultSchema
    }

  }

  def getSchema(
      project: String,
      topic: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String): StructType = {
    var schema = new StructType()
    schema = schema.add(StructField(PROJECT, StringType, false))
    schema = schema.add(StructField(TOPIC, StringType, false))
    schema = schema.add(StructField(SHARD, StringType, false))
    schema = schema.add(StructField(SYSTEM_TIME, TimestampType, false))

    val client = DatahubOffsetReader.getOrCreateDatahubClient(accessKeyId, accessKeySecret, endpoint)
    client.getTopic(project, topic).getRecordSchema.getFields.foreach(field => {
      schema = schema.add(StructField(field.getName, StringType, false))
    })

    schema
  }

  def validateOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    caseInsensitiveParams.getOrElse("project",
      throw new MissingArgumentException("Missing datahub project (='project')."))
    caseInsensitiveParams.getOrElse("topic",
      throw new MissingArgumentException("Missing datahub topic (='topic')."))
    caseInsensitiveParams.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    caseInsensitiveParams.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    caseInsensitiveParams.getOrElse("endpoint",
      throw new MissingArgumentException("Missing datahub endpoint (='endpoint')."))
    caseInsensitiveParams.getOrElse("zookeeper.connect.address",
      throw new MissingArgumentException("Missing zookeeper connect address " +
        "(='zookeeper.connect.address')."))
  }
}