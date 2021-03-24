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

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConverters._

import com.aliyun.datahub.model.RecordEntry

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

case class DatahubBatchInputPartition(
    offsetRange: DatahubOffsetRange,
    failOnDataLoss: Boolean,
    sourceOptions: Map[String, String],
    schemaDdl: String)
  extends InputPartition

object DatahubBatchReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[DatahubBatchInputPartition]
    DatahubBatchInputPartitionReader(
      p.offsetRange,
      p.failOnDataLoss,
      p.sourceOptions,
      p.schemaDdl)
  }
}

private case class DatahubBatchInputPartitionReader(
    offsetRange: DatahubOffsetRange,
    failOnDataLoss: Boolean,
    sourceOptions: Map[String, String],
    schemaDdl: String)
  extends PartitionReader[InternalRow] with Logging {

  private val accessKeyId = sourceOptions("access.key.id")
  private val accessKeySecret = sourceOptions("access.key.secret")
  private val endpoint = sourceOptions("endpoint")
  private val project = offsetRange.datahubShard.project
  private val topic = offsetRange.datahubShard.topic
  private val shardId = offsetRange.datahubShard.shardId
  @transient private val datahubClientAgent =
    DatahubOffsetReader.getOrCreateDatahubClient(accessKeyId, accessKeySecret, endpoint)

  private val step = 100
  private var hasRead = 0
  private val dataBuffer = new LinkedBlockingQueue[RecordEntry](step)

  @transient private val schema: StructType = StructType.fromDDL(schemaDdl)
  private val converter: DatahubRecordToUnsafeRowConverter =
    new DatahubRecordToUnsafeRowConverter(schema, sourceOptions)
  private var nextOffset = offsetRange.fromOffset
  private var nextCursor: String =
    datahubClientAgent.getCursor(project, topic, shardId, nextOffset).getCursor
  private var nextRow: UnsafeRow = _

  override def next(): Boolean = {
    // read range: [start, end)
    if (nextOffset < offsetRange.untilOffset) {
      if (dataBuffer.isEmpty) {
        fetchData()
      }

      if (dataBuffer.isEmpty) {
        false
      } else {
        val record = dataBuffer.poll()
        nextRow = converter.toUnsafeRow(record, project, topic, shardId)
        nextOffset += 1
        true
      }
    } else {
      false
    }
  }

  private def fetchData(): Unit = {
    val topicResult = datahubClientAgent.getTopic(project, topic)
    val recordSchema = topicResult.getRecordSchema
    val limit = if (offsetRange.untilOffset - nextOffset >= step) {
      step
    } else {
      offsetRange.untilOffset - nextOffset
    }
    val recordResult = datahubClientAgent
      .getRecords(project, topic, shardId, nextCursor, limit.toInt, recordSchema)
    recordResult.getRecords.asScala.foreach(record => {
      dataBuffer.offer(record)
    })
    nextCursor = recordResult.getNextCursor
    hasRead = hasRead + recordResult.getRecordCount
    logDebug(s"shardId: $shardId, nextCursor: $nextCursor, hasRead: $hasRead, " +
      s"total: ${offsetRange.untilOffset - offsetRange.fromOffset}")

  }

  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    datahubClientAgent.close()
  }
}
