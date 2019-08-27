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

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.JavaConversions._

import com.aliyun.datahub.model.OffsetContext
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

class DatahubSourceRDD(
    @transient _sc: SparkContext,
    endpoint: String,
    project: String,
    topic: String,
    accessId: String,
    accessKey: String,
    schemaFieldNames: Array[String],
    shardOffsets: Array[(String, Long, Long)],
    zkParam: Map[String, String],
    checkpointDir: String,
    maxOffsetPerTrigger: Long = -1) extends RDD[DatahubData](_sc, Nil) with Logging {

  @transient private var zkClient = DatahubOffsetReader.getOrCreateZKClient(zkParam)
  @transient private var datahubClientAgent = DatahubOffsetReader.getOrCreateDatahubClient(accessId, accessKey, endpoint)

  override def compute(split: Partition, context: TaskContext): Iterator[DatahubData] = {
    val shardPartition = split.asInstanceOf[ShardPartition]

    zkClient = DatahubOffsetReader.getOrCreateZKClient(zkParam)
    zkClient.setZkSerializer(new ZkSerializer{
      override def serialize(data: scala.Any): Array[Byte] = {
        data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        new String(bytes, StandardCharsets.UTF_8)
      }
    })
    datahubClientAgent = DatahubOffsetReader.getOrCreateDatahubClient(accessId, accessKey, endpoint)

    val schemaFieldPos: Map[String, Int] = schemaFieldNames.zipWithIndex.toMap
    try {
      new InterruptibleIterator[DatahubData](context, new NextIterator[DatahubData] {
        private val step = 100
        private var dataBuffer = new LinkedBlockingQueue[DatahubData](step)
        private var hasRead = 0
        private var lastOffset: OffsetContext.Offset = null
        private val inputMetrics = context.taskMetrics().inputMetrics
        private var nextCursor = shardPartition.cursor

        override protected def getNext(): DatahubData = {
          finished = !checkHasNext
          if (!finished) {
            if (dataBuffer.isEmpty) {
              fetchData()
            }
            if (dataBuffer.isEmpty) {
              finished = true
              null.asInstanceOf[DatahubData]
            } else {
              dataBuffer.poll()
            }
          } else {
            null.asInstanceOf[DatahubData]
          }
        }

        override protected def close() = {
          try {
            inputMetrics.incRecordsRead(hasRead)
            dataBuffer.clear()
            dataBuffer = null
          } catch {
            case e: Exception =>
              logError("Catch exception when close datahub iterator.", e)
          }
        }

        private def checkHasNext: Boolean = {
          if (shardPartition.count <= 0 ) {
            dataBuffer.nonEmpty
          } else {
            val hasNext = hasRead < shardPartition.count || !dataBuffer.isEmpty
            if (!hasNext) {
              // commit next offset
              val nextSeq = lastOffset.getSequence + 1
              writeDataToZk(zkClient, s"$checkpointDir/datahub/available/$project/$topic/${shardPartition.shardId}",
                nextSeq.toString)
            }
            hasNext
          }
        }

        private def fetchData() = {
          val topicResult = datahubClientAgent.getTopic(project, topic)
          val schema = topicResult.getRecordSchema
          val limit = if (shardPartition.count - hasRead >= step) step else shardPartition.count - hasRead
          val recordResult = datahubClientAgent.getRecords(project, topic, shardPartition.shardId, nextCursor,
            limit, schema)
          recordResult.getRecords.foreach(record => {
            try {
              val columnArray = Array.tabulate(schemaFieldNames.length)(_ =>
                (null, null).asInstanceOf[(String, Any)]
              )

              record.getFields.foreach(field => {
                val fieldName = field.getName
                columnArray(schemaFieldPos(fieldName)) = (fieldName, record.get(fieldName))
              })
              dataBuffer.offer(new SchemaDatahubData(project, topic, shardPartition.shardId,
                new Timestamp(record.getSystemTime), columnArray))
            } catch {
              case e: NoSuchElementException =>
                logWarning(s"Meet an unknown column name, ${e.getMessage}. Treat this as an invalid " +
                  s"data and continue.")
            }
            lastOffset = record.getOffset
          })
          nextCursor = recordResult.getNextCursor
          hasRead = hasRead + recordResult.getRecordCount
          logDebug(s"shardId: ${shardPartition.shardId}, nextCursor: $nextCursor, hasRead: $hasRead, count: ${shardPartition.count}")

        }

        private def writeDataToZk(zkClient: ZkClient, path:String, data:String) = {
          val dir = new Path(path).toUri.getPath
          if (!zkClient.exists(dir)) {
            zkClient.createPersistent(dir, true)
          }
          zkClient.writeData(dir, data)
        }
      })
    } catch {
      case e: Exception =>
        logError("Fail to build DatahubIterator.", e)
        Iterator.empty.asInstanceOf[Iterator[DatahubData]]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    datahubClientAgent = DatahubOffsetReader.getOrCreateDatahubClient(accessId, accessKey, endpoint)
    shardOffsets.zipWithIndex.map {
      case (so, index) =>
        val shardId = so._1
        val startOffset = so._2
        val endOffset = so._3
        var count = endOffset - startOffset + 1
        if (maxOffsetPerTrigger > 0 && maxOffsetPerTrigger < count) {
          count = maxOffsetPerTrigger
        }
        val cursor = datahubClientAgent.getCursor(project, topic, shardId, startOffset).getCursor
        new ShardPartition(id, shardId, index, cursor, count.toInt).asInstanceOf[Partition]
    }
  }
}

private class ShardPartition(
    rddId: Int,
    val shardId: String,
    partitionId: Int,
    val cursor: String,
    val count: Int) extends Partition {
  override def index: Int = partitionId
  override def hashCode(): Int = 41 * (41 + rddId) + index
}
