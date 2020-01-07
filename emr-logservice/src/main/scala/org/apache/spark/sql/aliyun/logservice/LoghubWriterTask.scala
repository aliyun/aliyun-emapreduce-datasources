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

import com.aliyun.openservices.aliyun.log.producer.{Callback, LogProducer, Result}
import com.aliyun.openservices.log.common.LogItem
import com.google.common.util.concurrent.ListenableFuture
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.streaming.aliyun.logservice.LoghubClientAgent

class LoghubWriterTask(
    sourceOptions: Map[String, String],
    inputSchema: Seq[Attribute]) extends LoghubGroupWriter(inputSchema) {
  val logServiceClient: LoghubClientAgent =
    LoghubOffsetReader.getOrCreateLoghubClient(sourceOptions)
  var producer: LogProducer = LoghubOffsetReader.getOrCreateLogProducer(sourceOptions)
  val logProject: String = sourceOptions.getOrElse("sls.project",
    throw new MissingArgumentException("Missing logService project (='sls.project')."))
  val logStore: String = sourceOptions.getOrElse("sls.store",
    throw new MissingArgumentException("Missing logService store (='sls.store')."))

  /**
   * Write data into log store
   */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    var lastSendResultFuture: ListenableFuture[Result] = null
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      lastSendResultFuture = sendRow(currentRow, producer, logProject, logStore)
    }
    if (lastSendResultFuture != null) {
      lastSendResultFuture.get()
      checkForErrors()
    }
  }

  def close(): Unit = {
    checkForErrors()
  }
}

abstract class LoghubGroupWriter(inputSchema: Seq[Attribute]) {
  @volatile protected var failedWrite: Exception = _
  private val schema = StructType(inputSchema.map(e =>
    StructField(e.name, e.dataType, e.nullable, e.metadata)))
  private val internalRowConverter = CatalystTypeConverters.createToScalaConverter(schema)
    .asInstanceOf[InternalRow => Row]
  private val converter: (Any) => Any = Utils.toConverter(schema)

  private val callback = new Callback() {
    override def onCompletion(result: Result): Unit = {
      if (failedWrite == null && !result.isSuccessful) {
        failedWrite = new Exception(s"Failed to send log, errorCode=${result.getErrorCode}, " +
          s"errorMessage=${result.getErrorMessage}, result=$result")
      }
    }
  }

  protected def sendRow(
      row: InternalRow,
      producer: LogProducer,
      logProject: String,
      logStore: String): ListenableFuture[Result] = {
    checkForErrors()
    val genericRecord = converter(internalRowConverter(row)).asInstanceOf[LogItem]
    producer.send(logProject, logStore, genericRecord, callback)
  }

  protected def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }
}
