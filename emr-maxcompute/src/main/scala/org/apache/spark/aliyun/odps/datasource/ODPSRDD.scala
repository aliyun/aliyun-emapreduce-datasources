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
package org.apache.spark.aliyun.odps.datasource

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang.StringUtils

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.aliyun.odps.OdpsPartition
import org.apache.spark.aliyun.odps.reader.ODPSTableIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class ODPSRDD(
    sc: SparkContext,
    schema: StructType,
    requiredPartition: String,
    options: ODPSOptions)
  extends RDD[InternalRow](sc, Nil) {

  @transient
  private lazy val sqlConf = SQLConf.get

  private val defaultParallelism = sc.defaultParallelism

  private val project = options.project
  private val table = options.table

  private val isPartitioned = options.odpsUtil.isPartitionTable(project, table)

  if (isPartitioned && (requiredPartition == null || requiredPartition.isEmpty)) {
    logWarning(s"Table $project.$table is partition table, but doesn't specify any partition")
  }

  /** Implemented by subclasses to compute a given partition. */
  override def compute(theSplit: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iter = new ODPSTableIterator(
      options.maxInFlight, theSplit.asInstanceOf[OdpsPartition], context, schema)
    new InterruptibleIterator[InternalRow](context, iter)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = {
    val start = System.nanoTime()

    val partitions = Option(requiredPartition).getOrElse("all").split(",")
      .filter(!StringUtils.isBlank(_))
      .map { spec =>
        val (numRecords, size) = if ("all".equalsIgnoreCase(spec)) {
          options.odpsUtil.getRecordCountAndSize(project, table)
        } else {
          options.odpsUtil.getRecordCountAndSize(project, table, spec)
        }
        logInfo(s"##### Table $project.$table partition $spec contains" +
          s" $numRecords records, total $size bytes.")
        (spec, (numRecords, size))
      }.filter(entry => entry._2._1 > 0 && entry._2._2 > 0)
      .toMap

    val defaultMaxSplitBytes = sqlConf.filesMaxPartitionBytes
    val openCostInBytes = sqlConf.filesOpenCostInBytes
    val totalBytes = partitions.map(_._2._2 + openCostInBytes).sum
    val bytesPerCore = totalBytes / defaultParallelism

    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
    logInfo(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
      s"open cost is considered as scanning $openCostInBytes bytes.")

    val odpsPartitions = new ArrayBuffer[OdpsPartition]
    var index = 0

    partitions.foreach {
      case (spec, (numRecords, size)) =>
        val partitionSpec = if (!"all".equalsIgnoreCase(spec)) spec else null
        if ((size + openCostInBytes) <= maxSplitBytes) {
          odpsPartitions += getPartition(index, 0, numRecords, partitionSpec)
          index += 1
        } else {
          val numSplits = Math.ceil(1.0 * (size + openCostInBytes) / maxSplitBytes).toInt
          val numRecordsPerSplit = Math.ceil(1.0 * numRecords / numSplits).toInt
          val numSplitWithSmallPart = numRecordsPerSplit * numSplits - numRecords
          val limit = (numSplits - numSplitWithSmallPart) * numRecordsPerSplit
          logInfo(s"##### numSplits is $numSplits, numRecordsPerSplit is $numRecordsPerSplit," +
            s" numSplitWithSmallPart is $numSplitWithSmallPart, limit is $limit.")

          0L.until(limit, numRecordsPerSplit).foreach { start =>
            odpsPartitions += getPartition(index, start, numRecordsPerSplit, partitionSpec)
            index += 1
          }

          val smallStep = numRecordsPerSplit - 1
          limit.until(numRecords, smallStep).foreach { start =>
            odpsPartitions += getPartition(index, start, smallStep, partitionSpec)
            index += 1
          }
        }
    }

    val end = System.nanoTime()
    logInfo(s"##### ODPSRDD.getPartitions cost ${end - start} ns.")

    odpsPartitions.foreach { p =>
      logInfo(s"##### Split $project.$table ($requiredPartition) get partition id ${p.idx}," +
        s" spec ${p.part}, range [${p.start}, ${p.start + p.count}).")
    }

    odpsPartitions.toArray
  }

  private def getPartition(
      index: Int, start: Long, count: Long, spec: String = null): OdpsPartition = {
    OdpsPartition(this.id, index, start, count, options.accessKeyId, options.accessKeySecret,
      options.odpsUrl, options.tunnelUrl, project, table, spec)
  }

  override def checkpoint(): Unit = {
    // Do nothing. ODPS RDD should not be checkpointed.
  }

}
