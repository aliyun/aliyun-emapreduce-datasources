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

import com.aliyun.odps.PartitionSpec
import org.apache.commons.lang.StringUtils

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.aliyun.odps.OdpsPartition
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class ODPSRDD(
    sc: SparkContext,
    schema: StructType,
    accessKeyId: String,
    accessKeySecret: String,
    odpsUrl: String,
    tunnelUrl: String,
    project: String,
    table: String,
    var requiredPartition: String,
    numPartitions: Int)
  extends RDD[InternalRow](sc, Nil) {

  private val isPartitioned = OdpsUtils(accessKeyId, accessKeySecret, odpsUrl)
    .isPartitionTable(table, project)

  if (isPartitioned && (requiredPartition == null || requiredPartition.isEmpty)) {
    logWarning(s"Table $project.$table is partition table, but doesn't specify any partition")
  }

  /** Implemented by subclasses to compute a given partition. */
  override def compute(theSplit: Partition, context: TaskContext): Iterator[InternalRow] = {
    val iter = new ODPSTableIterator(theSplit.asInstanceOf[OdpsPartition], context, schema)
    new InterruptibleIterator[InternalRow](context, iter)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = {
    val tunnel = OdpsUtils(accessKeyId, accessKeySecret, odpsUrl).getTableTunnel(tunnelUrl)
    val partitions = if (!isPartitioned) {
      val session = tunnel.createDownloadSession(project, table)
      val numRecords = session.getRecordCount
      logInfo(s"Table $project.$table contains $numRecords line data.")

      val finalPartitionNumber = math.max(1, math.min(numPartitions, numRecords)).toInt
      val range = getRanges(0, numRecords, finalPartitionNumber)

      Array.tabulate(finalPartitionNumber) {
        idx =>
          val (start, end) = range(idx)
          val count = (end - start).toInt
          OdpsPartition(this.id, idx, start, count, accessKeyId, accessKeySecret,
            odpsUrl, tunnelUrl, project, table, null)
      }.filter(p => p.count > 0)
        // remove the last count==0 to prevent exceptions from reading odps table.
        .map(_.asInstanceOf[Partition])
    } else {
      val partitionSpecs = requiredPartition.split(",")
        .filter(!StringUtils.isBlank(_))
        .map { spec =>
          val partition = new PartitionSpec(spec)
          val session = tunnel.createDownloadSession(project, table, partition)
          val numRecords = session.getRecordCount
          logInfo(s"Table $project.$table partition $spec contains $numRecords line data.")
          (spec, numRecords)
        }.filter(entry => entry._2 > 0)
        .toMap

      val numPartitionSpec = partitionSpecs.size
      if (numPartitionSpec >= numPartitions) {
        // If the size of partition needed to be read is bigger than numPartitions,
        // replace numPartitionSpec to numPartitions.
        partitionSpecs.zipWithIndex.map {
          case ((spec, numRecords), idx: Int) =>
            OdpsPartition(this.id, idx, 0, numRecords, accessKeyId, accessKeySecret,
              odpsUrl, tunnelUrl, project, table, spec).asInstanceOf[Partition]
        }.toArray
      } else {
        val totalReadableRecords = partitionSpecs.values.sum
        val readableRecordsPerPartition = math.max(1, totalReadableRecords / numPartitions)

        partitionSpecs.flatMap {
          case (spec, numRecords) =>
            0.asInstanceOf[Long].until(numRecords, readableRecordsPerPartition).map { start =>
              (spec, start, math.min(start + readableRecordsPerPartition, numRecords))
            }
        }.zipWithIndex.map {
          case ((spec, start, end), idx: Int) =>
            OdpsPartition(this.id, idx, start, end - start, accessKeyId, accessKeySecret,
              odpsUrl, tunnelUrl, project, table, spec).asInstanceOf[Partition]
        }.toArray
      }
    }
    logInfo(s"Split table $project.$table into ${partitions.length} partition(s).")
    partitions
  }

  /**
   * calculate the detail [start, end) of every region.
   * @param min min value
   * @param max max value
   * @param numRanges how many region will be split.
   * @return
   */
  private def getRanges(min: Long, max: Long, numRanges: Int): Array[(Long, Long)] = {
    val span = max - min
    val initSize = span / numRanges
    val sizes = Array.fill(numRanges)(initSize)
    val remainder = span - numRanges * initSize
    for (i <- 0 until remainder.toInt) {
      sizes(i) += 1
    }
    assert(sizes.sum == span)
    var start = min
    val ranges = sizes.map { size =>
      val current = start
      start += size
      (current, current + size)
    }
    assert(start == max)
    ranges
  }


  override def checkpoint(): Unit = {
    // Do nothing. ODPS RDD should not be checkpointed.
  }

}
