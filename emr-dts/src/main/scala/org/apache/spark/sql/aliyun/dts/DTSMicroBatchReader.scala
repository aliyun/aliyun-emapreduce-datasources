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
package org.apache.spark.sql.aliyun.dts

import java.io._
import java.nio.charset.StandardCharsets
import java.util
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, SerializedOffset}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types._
import org.apache.spark.util.UninterruptibleThread

class DTSMicroBatchReader(
    dtsOffsetReader: DTSOffsetReader,
    metadataPath: String,
    startingOffsets: DTSOffsetRangeLimit,
    options: DataSourceOptions) extends MicroBatchReader with Logging {

  private var startPartitionOffset: PartitionOffset = _
  private var endPartitionOffset: PartitionOffset = _

  private val maxOffsetsPerTrigger =
    Option(options.get("maxOffsetsPerTrigger").orElse(null)).map(_.toLong)

  private lazy val initialPartitionOffsets = getOrCreateInitialPartitionOffsets()

  override def getEndOffset: Offset = {
    DTSSourceOffset(endPartitionOffset)
  }

  override def getStartOffset: Offset = {
    DTSSourceOffset(startPartitionOffset)
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    startPartitionOffset = Option(start.orElse(null))
      .map(_.asInstanceOf[DTSSourceOffset].partitionToOffsets)
      .getOrElse(initialPartitionOffsets)

    endPartitionOffset = Option(end.orElse(null))
      .map(_.asInstanceOf[DTSSourceOffset].partitionToOffsets)
      .getOrElse {
        val latestPartitionOffsets = dtsOffsetReader.fetchLatestOffsets()
        maxOffsetsPerTrigger.map { maxOffsets =>
          rateLimit(maxOffsets, startPartitionOffset, latestPartitionOffsets)
        }.getOrElse {
          latestPartitionOffsets
        }
      }
  }

  override def commit(end: Offset): Unit = {}

  override def deserializeOffset(json: String): Offset = {
    DTSSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def readSchema(): StructType = DTSSourceProvider.getSchema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    val fromOffsets = startPartitionOffset
    val untilOffsets = endPartitionOffset

    Seq(
      new DTSMicroBatchInputPartition(
        fromOffsets._1,
        fromOffsets._2,
        untilOffsets._2,
        options.asMap()
      ).asInstanceOf[InputPartition[InternalRow]]
    ).asJava
  }

  override def stop(): Unit = {
    dtsOffsetReader.close()
  }

  override def toString: String = s"DTS[$dtsOffsetReader]"

  private def rateLimit(
      limit: Long,
      from: PartitionOffset,
      until: PartitionOffset): PartitionOffset = {
    val lag = until._2 - from._2
    if (lag >= limit) {
      (from._1, from._2 + limit)
    } else {
      until
    }
  }

  private def getOrCreateInitialPartitionOffsets(): PartitionOffset = {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    // SparkSession is required for getting Hadoop configuration for writing to checkpoints
    assert(SparkSession.getActiveSession.nonEmpty)

    val metadataLog =
      new KafkaSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit =>
          DTSSourceOffset(dtsOffsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit =>
          DTSSourceOffset(dtsOffsetReader.fetchLatestOffsets())
        case SpecificOffsetRangeLimit(p) =>
          DTSSourceOffset(dtsOffsetReader.fetchSpecificOffsets(p))
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  /** A version of [[HDFSMetadataLog]] specialized for saving the initial offsets. */
  class KafkaSourceInitialOffsetWriter(sparkSession: SparkSession, metadataPath: String)
    extends HDFSMetadataLog[DTSSourceOffset](sparkSession, metadataPath) {

    val VERSION = 1

    override def serialize(metadata: DTSSourceOffset, out: OutputStream): Unit = {
      out.write(0) // A zero byte is written to support Spark 2.1.0 (SPARK-19517)
      val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      writer.write("v" + VERSION + "\n")
      writer.write(metadata.json)
      writer.flush
    }

    override def deserialize(in: InputStream): DTSSourceOffset = {
      in.read() // A zero byte is read to support Spark 2.1.0 (SPARK-19517)
      val content = IOUtils.toString(new InputStreamReader(in, StandardCharsets.UTF_8))
      // HDFSMetadataLog guarantees that it never creates a partial file.
      assert(content.length != 0)
      if (content(0) == 'v') {
        val indexOfNewLine = content.indexOf("\n")
        if (indexOfNewLine > 0) {
          val version = parseVersion(content.substring(0, indexOfNewLine), VERSION)
          DTSSourceOffset(SerializedOffset(content.substring(indexOfNewLine + 1)))
        } else {
          throw new IllegalStateException(
            s"Log file was malformed: failed to detect the log file version line.")
        }
      } else {
        // The log was generated by Spark 2.1.0
        DTSSourceOffset(SerializedOffset(content))
      }
    }
  }
}
