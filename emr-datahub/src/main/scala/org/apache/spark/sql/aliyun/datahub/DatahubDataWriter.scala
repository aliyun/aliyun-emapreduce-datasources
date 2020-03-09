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

import com.aliyun.datahub.client.DatahubClient
import com.aliyun.datahub.client.exception._
import com.aliyun.datahub.client.model._
import scala.collection._
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._


// One Spark task has one exclusive data writer, so there is no thread-safe concern.
class DatahubDataWriter(
    project: Option[String],
    topic: Option[String],
    sourceOptions: immutable.Map[String, String],
    schema: Option[StructType]) extends DataWriter[InternalRow] with Logging {

  private val client = DatahubSourceProvider.getOrCreateDatahubClientV2(sourceOptions)

  lazy val getRecordResult = client.getTopic(project.get, topic.get)
  lazy val recordSchema: RecordSchema = getRecordResult.getRecordSchema

  private lazy val precision = sourceOptions("decimal.precision").toInt
  private lazy val scale = sourceOptions("decimal.scale").toInt

  lazy val asyncWriter: DatahubAsyncDataWriter =
    getAsyncWriter(client, project.get, topic.get, sourceOptions)

  override def commit(): WriterCommitMessage = {
    checkError()
    asyncWriter.flush()
    asyncWriter.waitCompleted()
    checkError()

    DatahubWriterCommitMessage
  }

  override def abort(): Unit = {
  }

  override def write(row: InternalRow): Unit = {
    try {
      val recordEntry = convertRowToRecordEntry(row)
      checkError()
      asyncWriter.putRecord(recordEntry)
    } catch {
      case ex: Exception =>
        logError(ex.getMessage)
        throw ex
    }
  }

  private def checkError(): Unit = {
    asyncWriter.checkStatus()

    if (asyncWriter.failedWrite != null) {
      asyncWriter.failedWrite match {
        case e: InvalidParameterException =>
          logError("Invalid parameter, please check your parameter." + e.getErrorMessage)
          throw e
        case e: AuthorizationFailureException =>
          logError("AK error, please check your accessId and accessKey" + e.getErrorMessage)
          throw e
        case e: ResourceNotFoundException =>
          logError("project or topic or shard is not found" + e.getErrorMessage)
          throw e
        case e: ShardSealedException =>
          logError("shard status is CLOSED, can not be writen" + e.getErrorMessage)
          throw e
        case e: DatahubClientException =>
          logError(e.getErrorMessage)
          throw e
      }
    }
  }

  private def convertRowToRecordEntry(row: InternalRow): RecordEntry = {

    val record: RecordEntry = new RecordEntry()
    val data: RecordData = getRecordResult.getRecordType match {
      case RecordType.BLOB => new BlobRecordData(row.getString(0).getBytes("UTF-8"))
      case RecordType.TUPLE =>
        val tuple = new TupleRecordData(recordSchema)
        var idx = 0
        recordSchema.getFields.asScala.foreach(field => {
          if (!row.isNullAt(idx)) {
            field.getType match {
              case FieldType.BIGINT => tuple.setField(idx, row.getLong(idx))
              case FieldType.TIMESTAMP => tuple.setField(idx, row.getLong(idx))
              case FieldType.BOOLEAN => tuple.setField(idx, row.getBoolean(idx))
              case FieldType.DECIMAL =>
                tuple.setField(idx, row.getDecimal(idx, precision, scale).toJavaBigDecimal)
              case FieldType.DOUBLE => tuple.setField(idx, row.getDouble(idx))
              case _ => tuple.setField(idx, row.getString(idx))
            }
          }
          idx = idx + 1
        })
        tuple
    }

    record.setRecordData(data)
    record
  }

  private def getAsyncWriter(
    client: DatahubClient,
    project: String,
    topic: String,
    sourceOptions: immutable.Map[String, String]): DatahubAsyncDataWriter = {

    val batchSize = sourceOptions.get(DatahubSourceProvider.OPTION_KEY_BATCH_SIZE).map(_.trim.toInt)
    val batchNum = sourceOptions.get(DatahubSourceProvider.OPTION_KEY_BATCH_NUM).map(_.trim.toInt)

    // by default, no batch
    val size = batchSize.getOrElse(1)

    // Future numbers
    val num = batchNum.getOrElse(5)

    new DatahubAsyncDataWriter(client, project, topic, size, num)
  }
}

