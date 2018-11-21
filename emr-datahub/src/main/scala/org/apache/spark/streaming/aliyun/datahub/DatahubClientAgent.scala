package org.apache.spark.streaming.aliyun.datahub

import com.aliyun.datahub.common.data.RecordSchema
import com.aliyun.datahub.exception.{InvalidParameterException, MalformedRecordException, ResourceNotFoundException}
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.{DatahubClient, DatahubConfiguration}
import com.aliyun.datahub.model._
import org.apache.spark.internal.Logging

/**
  * @date 2018/09/01
  */
class DatahubClientAgent(conf: DatahubConfiguration) extends Logging {

  private val datahubServiceMaxRetry = 3
  private val client = new DatahubClient(conf)

  def getTopic(projectName: String, topicName: String): GetTopicResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        return client.getTopic(projectName, topicName)
      catch {
        case e: ResourceNotFoundException => {
          throw e
        }
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when getTopic, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to getTopic exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def getCursor(projectName: String, topicName: String, shardId: String, cursorType: CursorType): GetCursorResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        return client.getCursor(projectName, topicName, shardId, cursorType)
      catch {
        case e: ResourceNotFoundException => {
          throw e
        }
        case e: InvalidParameterException => {
          throw e
        }
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when getCursor, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to getCursor exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def initOffsetContext(projectName: String, topicName: String, subscribeId: String, shardId: String): OffsetContext = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        return client.initOffsetContext(projectName, topicName, subscribeId, shardId)
      catch {
        case e: InvalidParameterException => {
          throw e
        }
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when initOffsetContext, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to initOffsetContext exceed max retry times[$datahubServiceMaxRetry")
    throw currentException
  }

  def getNextOffsetCursor(offsetCtx: OffsetContext): GetCursorResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        return client.getNextOffsetCursor(offsetCtx)
      catch {
        case e: ResourceNotFoundException => {
          throw e
        }
        case e: InvalidParameterException => {
          throw e
        }
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when getNextOffsetCursor, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to getNextOffsetCursor exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def commitOffset(offsetCtx: OffsetContext): CommitOffsetResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        return client.commitOffset(offsetCtx)
      catch {
        case e: InvalidParameterException => {
          throw e
        }
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when commitOffset, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to commitOffset exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def updateOffsetContext(offsetCtx: OffsetContext): Unit = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        client.updateOffsetContext(offsetCtx)
      catch {
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when updateOffsetContext, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to updateOffsetContext exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }

  def getRecords(projectName: String, topicName: String, shardId: String, cursor: String, limit: Int, schema: RecordSchema): GetRecordsResult = {
    var retry = 0
    var currentException: Exception = null
    while (retry <= datahubServiceMaxRetry) {
      try
        return client.getRecords(projectName, topicName, shardId, cursor, limit, schema)
      catch {
        case e: MalformedRecordException => {
          throw e
        }
        case e: ResourceNotFoundException => {
          throw e
        }
        case e: InvalidParameterException => {
          throw e
        }
        case e: Exception => {
          retry += 1
          currentException = e
          logError(s"catch exception when getRecords, start to retry $retry-times")
        }
      }
    }
    logError(s"retry to getRecords exceed max retry times[$datahubServiceMaxRetry].")
    throw currentException
  }
}
