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

package org.apache.spark.streaming.aliyun.datahub

import com.aliyun.datahub.{DatahubClient, DatahubConfiguration}
import com.aliyun.datahub.common.data.RecordSchema
import com.aliyun.datahub.exception.{InvalidParameterException, MalformedRecordException, ResourceNotFoundException}
import com.aliyun.datahub.model._
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ObjectNode

import org.apache.spark.internal.Logging

class DatahubClientAgent(conf: DatahubConfiguration) extends Logging {

  private val datahubServiceMaxRetry = 3
  private[spark] val client = new DatahubClient(conf)

  def getTopic(projectName: String, topicName: String): GetTopicResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
          return client.getTopic(projectName, topicName)
        }
      catch {
        case e: ResourceNotFoundException =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when getTopic, start to retry $retry-times")
      }
    }
    logError(s"retry to getTopic exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def getCursor(projectName: String, topicName: String, shardId: String, cursorType: CursorType):
    GetCursorResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        return client.getCursor(projectName, topicName, shardId, cursorType)
      }
      catch {
        case e @ (_: ResourceNotFoundException | _: InvalidParameterException ) =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when getCursor, start to retry $retry-times")
      }
    }
    logError(s"retry to getCursor exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def getCursor(
      projectName: String,
      topicName: String,
      shardId: String,
      offset: OffsetContext.Offset): GetCursorResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        val request = new GetCursorRequest(projectName, topicName, shardId,
          CursorType.SEQUENCE, offset.getSequence)
        return client.getCursor(request)
      }
      catch {
        case e @ (_: ResourceNotFoundException | _: InvalidParameterException) =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when getCursor, start to retry $retry-times")
      }
    }
    logError(s"retry to getCursor exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def getCursor(
      projectName: String,
      topicName: String,
      shardId: String,
      sequence: Long): GetCursorResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        val request =
          new GetCursorRequest(projectName, topicName, shardId, CursorType.SEQUENCE, sequence)
        return client.getCursor(request)
      }
      catch {
        case e @ (_: ResourceNotFoundException | _: InvalidParameterException) =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when getCursor, start to retry $retry-times")
      }
    }
    logError(s"retry to getCursor exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def getRecords(
      projectName: String,
      topicName: String,
      shardId: String,
      cursor: String,
      limit: Int,
      schema: RecordSchema): GetRecordsResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        return client.getRecords(projectName, topicName, shardId, cursor, limit, schema)
      }
      catch {
        case e @ (_: MalformedRecordException |
                  _: ResourceNotFoundException |
                  _: InvalidParameterException) => {
          throw e
        }
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when getRecords, start to retry $retry-times")
      }
    }
    logError(s"retry to getRecords exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def listShards(project: String, topic: String): ListShardResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        return client.listShard(project, topic)
      }
      catch {
        case e: InvalidParameterException =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when listShards, start to retry $retry-times")
      }
    }
    logError(s"retry to listShards exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def initOffsetContext(
      projectName: String,
      topicName: String,
      subscribeId: String,
      shardId: String): OffsetContext = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        return client.initOffsetContext(projectName, topicName, subscribeId, shardId)
      }
      catch {
        case e: InvalidParameterException =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when initOffsetContext, start to retry $retry-times")
      }
    }
    logError(s"retry to initOffsetContext exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  /**
   * @throws InvalidParameterException
   *        one possible reason is when timestamp of oldest data in datahub
   *        is larger than consume offset in offsetCtx cause data is expired
   */
  def getNextOffsetCursor(offsetCtx: OffsetContext): GetCursorResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        return client.getNextOffsetCursor(offsetCtx)
      }
      catch {
        case e @ (_: ResourceNotFoundException | _: InvalidParameterException) =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when getNextOffsetCursor, start to retry $retry-times")
      }
    }
    logError(s"retry to getNextOffsetCursor exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def commitOffset(offsetCtx: OffsetContext): CommitOffsetResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        return client.commitOffset(offsetCtx)
      }
      catch {
        case e: InvalidParameterException =>
          throw e
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when commitOffset, start to retry $retry-times")
      }
    }
    logError(s"retry to commitOffset exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def updateOffsetContext(offsetCtx: OffsetContext): Unit = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try {
        client.updateOffsetContext(offsetCtx)
      }
      catch {
        case e: Exception =>
          retry += 1
          currentException = e
          logError(s"catch exception when updateOffsetContext, start to retry $retry-times")
      }
    }
    logError(s"retry to updateOffsetContext exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def close(): Unit = {
    client.close()
  }
}

object JacksonParser {
  val objectMapper = new ObjectMapper()

  def getOffsetContext(offsetContextObjectNode: JsonNode): OffsetContext = {
    offsetContextObjectNode.getBinaryValue
    val offset = new OffsetContext.Offset(offsetContextObjectNode.get("Sequence").asLong(),
      offsetContextObjectNode.get("Timestamp").asLong())
    new OffsetContext(
      offsetContextObjectNode.get("Project").asText(),
      offsetContextObjectNode.get("Topic").asText(),
      offsetContextObjectNode.get("SubId").asText(),
      offsetContextObjectNode.get("ShardId").asText(),
      offset,
      offsetContextObjectNode.get("Version").asLong(),
      offsetContextObjectNode.get("SessionId").asText())
  }

  def getOffsetContext(offsetContextStr: String): OffsetContext = {
    getOffsetContext(objectMapper.readTree(offsetContextStr))
  }

  def getOffset(offsetStr: String): OffsetContext.Offset = {
    getOffset(objectMapper.readTree(offsetStr))
  }

  def getOffset(offset: JsonNode): OffsetContext.Offset = {
    new OffsetContext.Offset(offset.get("Sequence").asLong(), offset.get("Timestamp").asLong())
  }

  def getOffset(sequenceId: Long, recordTime: Long): OffsetContext.Offset = {
    new OffsetContext.Offset(sequenceId, recordTime)
  }

  def toJsonNode(offset: OffsetContext.Offset): ObjectNode = {
    val node = objectMapper.createObjectNode()
    node.put("Sequence", offset.getSequence)
    node.put("Timestamp", offset.getTimestamp)
    node
  }
}
