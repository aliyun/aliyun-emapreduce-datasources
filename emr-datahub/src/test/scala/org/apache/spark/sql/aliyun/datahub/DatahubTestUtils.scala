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

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.aliyun.datahub.DatahubClient
import com.aliyun.datahub.client.model.{Field => CField, FieldType => CFieldType, RecordSchema => CRecordSchema}
import com.aliyun.datahub.common.data.{Field, FieldType, RecordSchema, RecordType}
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.model.RecordEntry

import org.apache.spark.sql.types._

class DatahubTestUtils {

  var client: DatahubClient = _
  val id = new AtomicInteger(0)

  val project: String = Option(System.getenv("DATAHUB_PROJECT_NAME")).getOrElse("")
  val accessKeyId: String = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
  val accessKeySecret: String = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")
  val region: String = Option(System.getenv("REGION_NAME")).getOrElse("cn-hangzhou")
  val envType: String = Option(System.getenv("TEST_ENV_TYPE")).getOrElse("public").toLowerCase

  if (envType != "private" && envType != "public") {
    throw new Exception(s"Unsupported test environment type: $envType, only support private or public")
  }

  val endpoint: String = Option(System.getenv("DATAHUB_ENDPOINT")).getOrElse {
    envType match {
      case "private" => s"http://dh-$region.aliyun-inc.com"
      case "public" => s"https://dh-$region.aliyuncs.com"
    }
  }

  private def newTopic(): String = s"topic_for_emr_sdk_ut_${id.incrementAndGet()}"

  def init(): Unit = {
    client = DatahubOffsetReader
      .getOrCreateDatahubClient(accessKeyId, accessKeySecret, endpoint)
      .client
  }

  def sendMessage(topic: String, shardId: Option[Int], message: String*): Unit = {
    val recordSchema = client.getTopic(project, topic).getRecordSchema
    val clientRecordSchema = new CRecordSchema
    recordSchema.getFields.asScala.foreach(f => {
      val fieldType: CFieldType = f.getType match {
        case FieldType.BIGINT => CFieldType.BIGINT
        case FieldType.DOUBLE => CFieldType.DOUBLE
        case FieldType.DECIMAL => CFieldType.DECIMAL
        case FieldType.BOOLEAN => CFieldType.BOOLEAN
        case FieldType.STRING => CFieldType.STRING
        case FieldType.TIMESTAMP => CFieldType.TIMESTAMP
      }

      clientRecordSchema.addField(new CField(f.getName, fieldType))
    })
    val recordEntries = new util.ArrayList[RecordEntry]()
    message.foreach(msg => {
      val recordEntry = new RecordEntry(recordSchema)
      recordEntry.setString(0, msg)
      if (shardId.isDefined) {
        recordEntry.setShardId(shardId.get.toString)
      }
      recordEntries.add(recordEntry)
    })
    client.putRecords(project, topic, recordEntries.asScala.toList.asJava, 10)
  }

  def createTopic(schema: StructType): String = {
    val topic = newTopic()
    val recordSchema = new RecordSchema
    schema.fields.foreach(field => {
      val fieldType = field.dataType match {
        case LongType => FieldType.BIGINT
        case DoubleType => FieldType.DOUBLE
        case _: DecimalType => FieldType.DECIMAL
        case BooleanType => FieldType.BOOLEAN
        case StringType => FieldType.STRING
        case TimestampType => FieldType.TIMESTAMP
      }
      recordSchema.addField(new Field(field.name, fieldType))
    })

    client.createTopic(project, topic, 2, 1, RecordType.TUPLE, recordSchema, "emr-sdk-ut")

    topic
  }

  def getLatestOffsets(topic: String): Map[DatahubShard, Long] = {
    val shards = client.listShard(project, topic).getShards.asScala
    shards.map(shard => {
      val cursor = client.getCursor(project, topic, shard.getShardId, CursorType.LATEST)
      (DatahubShard(project, topic, shard.getShardId), cursor.getSequence)
    }).toMap
  }

  def getAllTopicAndShardSize(): Map[DatahubInfo, Int] = {
    val topics = client.listTopic(project).getTopics.asScala
    topics.map(t => {
      val shards = client.listShard(project, t).getShards.asScala
      (DatahubInfo(project, t), shards.size)
    }).toMap
  }

  def cleanAllResource(): Unit = {
    val allTopics = client.listTopic(project)
    allTopics.getTopics.asScala.foreach(t => {
      client.deleteTopic(project, t)
    })
  }
}
