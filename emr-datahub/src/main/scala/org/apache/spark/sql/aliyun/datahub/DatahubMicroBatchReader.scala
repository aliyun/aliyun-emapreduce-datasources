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

import java.{util => ju}
import java.io._
import java.nio.charset.StandardCharsets
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.aliyun.datahub.DatahubSourceProvider._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, SerializedOffset}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.StructType

class DatahubMicroBatchReader(
    @transient offsetReader: DatahubOffsetReader,
    @transient sourceOptions: DataSourceOptions,
    metadataPath: String,
    startingOffsets: DatahubOffsetRangeLimit,
    failOnDataLoss: Boolean,
    userSpecifiedSchemaDdl: Option[String])
  extends MicroBatchReader with Serializable with Logging {

  private val userSpecifiedSchema =
    if (userSpecifiedSchemaDdl.isDefined && userSpecifiedSchemaDdl.get.nonEmpty) {
      userSpecifiedSchemaDdl.map(StructType.fromDDL)
    } else {
      Some(new StructType())
    }

  private var startPartitionOffsets: Map[DatahubShard, Long] = _
  private var endPartitionOffsets: Map[DatahubShard, Long] = _
  private val maxOffsetsPerTrigger =
    Option(sourceOptions.get("maxOffsetsPerTrigger").orElse(null)).map(_.toLong)

  private lazy val initialPartitionOffsets = getOrCreateInitialPartitionOffsets()

  private def getOrCreateInitialPartitionOffsets(): Map[DatahubShard, Long] = {
    // SparkSession is required for getting Hadoop configuration for writing to checkpoints
    assert(SparkSession.getActiveSession.nonEmpty)

    val metadataLog =
      new DatahubSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case OldestOffsetRangeLimit =>
          DatahubSourceOffset(offsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit =>
          DatahubSourceOffset(offsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(p) =>
          DatahubSourceOffset(p.toSeq.map(i => (i._1.project, i._1.topic, i._1.shardId, i._2)): _*)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.shardToOffsets
  }

  override def getEndOffset: Offset = {
    DatahubSourceOffset(endPartitionOffsets)
  }

  override def getStartOffset: Offset = {
    DatahubSourceOffset(startPartitionOffsets)
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    startPartitionOffsets = Option(start.orElse(null))
      .map(_.asInstanceOf[DatahubSourceOffset].shardToOffsets)
      .getOrElse(initialPartitionOffsets)

    // todo: if shard merged?
    endPartitionOffsets = Option(end.orElse(null))
      .map(_.asInstanceOf[DatahubSourceOffset].shardToOffsets)
      .getOrElse {
        val latestPartitionOffsets = offsetReader.fetchLatestOffsets(Some(startPartitionOffsets))
        maxOffsetsPerTrigger.map { maxOffsets =>
          rateLimit(maxOffsets, startPartitionOffsets, latestPartitionOffsets)
        }.getOrElse {
          latestPartitionOffsets
        }
      }
  }

  private def rateLimit(
      limit: Long,
      from: Map[DatahubShard, Long],
      until: Map[DatahubShard, Long]): Map[DatahubShard, Long] = {
    val fromNew = offsetReader.fetchEarliestOffsets()
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
          val size = end - begin
          logDebug(s"rateLimit $tp size is $size")
          if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes.get(tp).map { size =>
            val begin = from.get(tp).getOrElse(fromNew(tp))
            val prorate = limit * (size / total)
            val prorateLong = (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            // need to be careful of integer overflow
            // therefore added canary checks where to see if off variable could be overflowed
            // refer to [https://issues.apache.org/jira/browse/SPARK-26718]
            val off = if (prorateLong > Long.MaxValue - begin) {
              Long.MaxValue
            } else {
              begin + prorateLong
            }
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  override def commit(end: Offset): Unit = {}

  override def deserializeOffset(json: String): Offset = {
    DatahubSourceOffset(DatahubSourceOffset.partitionOffsets(json))
  }

  override def readSchema(): StructType = {
    DatahubSchema.getSchema(userSpecifiedSchema, sourceOptions.asMap().asScala.toMap)
  }

  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE")
    } else {
      logWarning(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE")
    }
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    // Find the new partitions, and get their earliest offsets
    val newPartitions = endPartitionOffsets.keySet.diff(startPartitionOffsets.keySet)
    val newPartitionInitialOffsets = offsetReader.fetchEarliestOffsets(newPartitions)
    logInfo(s"Partitions added: $newPartitionInitialOffsets")

    // Find deleted partitions, and report data loss if required
    val deletedPartitions = startPartitionOffsets.keySet.diff(endPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    // Use the end partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = endPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionInitialOffsets.contains(tp) || startPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    val fromOffsets = startPartitionOffsets ++ newPartitionInitialOffsets
    val untilOffsets = endPartitionOffsets
    untilOffsets.foreach { case (tp, untilOffset) =>
      fromOffsets.get(tp).foreach { fromOffset =>
        if (untilOffset < fromOffset) {
          reportDataLoss(s"Partition $tp's offset was changed from " +
            s"$fromOffset to $untilOffset, some data may have been missed")
        }
      }
    }

    val partitionsToRead = untilOffsets.keySet.intersect(fromOffsets.keySet)

    val offsetRanges = partitionsToRead.toSeq.map { tp =>
      DatahubOffsetRange(tp, fromOffsets(tp), untilOffsets(tp))
    }.filter(_.size > 0)

    // Generate factories based on the offset ranges
    offsetRanges.map { range =>
      new DatahubMicroBatchInputPartition(range, failOnDataLoss,
        sourceOptions.asMap().asScala.toMap, readSchema().toDDL): InputPartition[InternalRow]
    }.asJava
  }

  override def stop(): Unit = {
    offsetReader.close()
  }

  override def toString(): String = s"DatahubV2[$offsetReader]"

  /** A version of [[HDFSMetadataLog]] specialized for saving the initial offsets. */
  class DatahubSourceInitialOffsetWriter(sparkSession: SparkSession, metadataPath: String)
    extends HDFSMetadataLog[DatahubSourceOffset](sparkSession, metadataPath) {

    val VERSION = 1

    override def serialize(metadata: DatahubSourceOffset, out: OutputStream): Unit = {
      out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
      val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      writer.write("v" + VERSION + "\n")
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
          val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
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
}
