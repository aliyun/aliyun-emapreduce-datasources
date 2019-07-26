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

import java.io._
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.cli.MissingArgumentException
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, Offset, SerializedOffset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

class DatahubSource(
    @transient sqlContext: SQLContext,
    userSpecifiedSchema: Option[StructType],
    sourceOptions: Map[String, String],
    metadataPath: String,
    @transient datahubOffsetReader: DatahubOffsetReader,
    startOffset: DatahubOffsetRangeLimit) extends Source with Serializable with Logging {
  private val currentBatches = new mutable.HashMap[(Offset, Offset), RDD[InternalRow]]()
  private var currentPartitionOffset: Option[Map[DatahubShard, Long]] = None
  private val zkParams = sourceOptions.filter(_._1.toLowerCase(Locale.ROOT).startsWith("zookeeper."))
    .map(option => option._1.drop(10) -> option._2)
  @transient private val zkClient = DatahubOffsetReader.getOrCreateZKClient(zkParams)
  private val maxOffsetsPerTrigger = sourceOptions.getOrElse("maxOffsetsPerTrigger", "10000").toLong
  private val endpoint = sourceOptions.getOrElse("endpoint",
    throw new MissingArgumentException("Missing datahub endpoint (='endpoint')."))
  private val project = sourceOptions.getOrElse("project",
    throw new MissingArgumentException("Missing datahub project (='project')."))
  private val topic = sourceOptions.getOrElse("topic",
    throw new MissingArgumentException("Missing datahub topic (='topic')."))
  private val accessKeyId = sourceOptions.getOrElse("access.key.id",
    throw new MissingArgumentException("Missing aliyun account access key id (='access.key.id')"))
  private val accessKeySecret = sourceOptions.getOrElse("access.key.secret",
    throw new MissingArgumentException("Missing aliyun access key secret (='access.key.secret')"))

  private lazy val initialPartitionOffsets = {
    val metadataLog = new HDFSMetadataLog[DatahubSourceOffset](sqlContext.sparkSession, metadataPath) {
      override def serialize(metadata: DatahubSourceOffset, out: OutputStream): Unit = {
        out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
        val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
        writer.write("v" + DatahubSource.VERSION + "\n")
        writer.write(metadata.json())
        writer.flush()
      }

      override def deserialize(in: InputStream): DatahubSourceOffset = {
        in.read() // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
        val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))

        // HDFSMetadataLog guarantees that it never creates a partial file.
        assert(content.length != 0)
        if (content(0) == 'v') {
          val indexOfNewLine = content.indexOf("\n")
          if (indexOfNewLine > 0) {
            DatahubSourceOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
          } else {
            throw new IllegalStateException(
              s"Log file was malformed: failed to detect the log file version line.")
          }
        } else {
          // The log was generated by Spark 2.1.0
          DatahubSourceOffset(SerializedOffset(content))
        }
      }
    }

    metadataLog.get(0).getOrElse({
      val offset = startOffset match {
        case OldestOffsetRangeLimit => DatahubSourceOffset(datahubOffsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => DatahubSourceOffset(datahubOffsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(_) => throw new UnsupportedEncodingException()
      }
      metadataLog.add(0, offset)
      logInfo(s"Initial offset: $offset")
      offset
    })
  }.shardToOffsets

  override def schema: StructType = DatahubSchema.getSchema(userSpecifiedSchema, sourceOptions)

  // TODO: remove fallback
  private val fallback = schema.sameType(DatahubSchema.getDefaultSchema)

  override def getOffset: Option[Offset] = {
    initialPartitionOffsets

    val latestOffset = datahubOffsetReader.fetchLatestOffsets()
    val earliestOffset = datahubOffsetReader.fetchEarliestOffsets()
    val shardOffsets = new collection.mutable.ArrayBuffer[(String, Long, Long)]()
    if (currentPartitionOffset.isEmpty) {
      initialPartitionOffsets.foreach(po => {
        shardOffsets += ((po._1.shardId, po._2, latestOffset(po._1)))
      })
      latestOffset.keySet.diff(initialPartitionOffsets.keySet).foreach(shard => {
        shardOffsets += ((shard.shardId, earliestOffset(shard), latestOffset(shard)))
      })
    } else {
      currentPartitionOffset.get.foreach(po => {
        shardOffsets += ((po._1.shardId, po._2, latestOffset(po._1)))
      })
      latestOffset.keySet.diff(currentPartitionOffset.get.keySet).foreach(shard => {
        shardOffsets += ((shard.shardId, earliestOffset(shard), latestOffset(shard)))
      })
    }

    // TODO: remove zk and pre-compute
    currentBatches.foreach(_._2.unpersist(false))
    currentBatches.clear()
    val rdd = new DatahubSourceRDD(sqlContext.sparkContext, endpoint, project, topic, accessKeyId,
      accessKeySecret, schema.fieldNames, shardOffsets.toArray, zkParams, metadataPath, maxOffsetsPerTrigger, fallback)
      .mapPartitions(it => {
        it.map(data => {
          if (fallback) {
            InternalRow(data.project,
              data.topic,
              data.shardId,
              data.systemTime,
              data.getContent)
          } else {
            RowEncoder(schema).resolveAndBind().toRow(new GenericRow(data.toArray))
          }
        })
      })
    sqlContext.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
    rdd.persist(StorageLevel.MEMORY_AND_DISK).count()

    val startOffset = DatahubSourceOffset(shardOffsets.map(so => (DatahubShard(project, topic, so._1), so._2)).toMap)
    val end = shardOffsets.map(so => {
      val path = new Path(metadataPath).toUri.getPath
      val available: String = zkClient.readData(s"$path/datahub/available/$project/$topic/${so._1}",
        true)
      val offset = if (available != null) {
        available.toLong
      } else {
        so._3
      }
      (DatahubShard(project, topic, so._1), offset)
    }).toMap
    val endOffset = DatahubSourceOffset(end)
    currentBatches((startOffset, endOffset)) = rdd
    currentPartitionOffset = Some(end)
    Some(endOffset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startOffset = if (start.isEmpty) {
      DatahubSourceOffset(initialPartitionOffsets)
    } else {
      start.get
    }

    val rdd = if (currentBatches.contains((startOffset, end))) {
      val expiredBatches = currentBatches.filter(b => !b._1._1.equals(startOffset) || !b._1._2.equals(end))
      expiredBatches.foreach(_._2.unpersist())
      expiredBatches.foreach(b => currentBatches.remove(b._1))
      currentBatches((startOffset, end))
    } else {
      val fromShardOffsets = start match {
        case Some(prevBatchEndOffset) =>
          DatahubSourceOffset.getShardOffsets(prevBatchEndOffset)
        case None =>
          initialPartitionOffsets
      }
      val shardOffsets = new ArrayBuffer[(String, Long, Long)]()
      val untilShardOffsets = DatahubSourceOffset.getShardOffsets(end)
      val shards = untilShardOffsets.keySet.filter { shard =>
        // Ignore partitions that we don't know the from offsets.
        fromShardOffsets.contains(shard)
      }.toSeq
      shards.foreach(shard => {
        shardOffsets.+=((shard.shardId, fromShardOffsets(shard), untilShardOffsets(shard)))
      })
      new DatahubSourceRDD(sqlContext.sparkContext, endpoint, project, topic, accessKeyId, accessKeySecret,
        schema.fieldNames, shardOffsets.toArray, zkParams, metadataPath, maxOffsetsPerTrigger, fallback)
        .mapPartitions(it => {
          val encoder = RowEncoder(schema).resolveAndBind(schema.toAttributes)
          it.map(data => {
            if (fallback) {
              InternalRow(data.project,
                data.topic,
                data.shardId,
                data.systemTime,
                data.getContent)
            } else {
              encoder.toRow(new GenericRow(data.toArray))
            }
          })
        })
    }

    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def stop(): Unit = {
    datahubOffsetReader.close()
  }
}

object DatahubSource {
  val VERSION = 1
}
