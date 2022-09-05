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

import org.apache.commons.lang.StringUtils

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.aliyun.odps.OdpsPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class ODPSRDD(
    sc: SparkContext,
    schema: StructType,
    var requiredPartition: String,
    options: ODPSOptions)
  extends RDD[InternalRow](sc, Nil) {

  private val project = options.project
  private val table = options.table

  private val isPartitioned = options.odpsUtil.isPartitionTable(project, table)

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
    val numPartitions = options.numPartitions

    val partitions = if (!isPartitioned) {
      val numRecords = options.odpsUtil.getRecordCount(project, table)
      logInfo(s"Table $project.$table contains $numRecords line data.")

      val finalPartitionNumber = math.max(1, math.min(numPartitions, numRecords)).toInt
      val range = getRanges(0, numRecords, finalPartitionNumber)

      Array.tabulate(finalPartitionNumber) {
        idx =>
          val (start, end) = range(idx)
          val count = (end - start).toInt
          getPartition(idx, start, count)
      }.filter(p => p.count > 0)
        // remove the last count==0 to prevent exceptions from reading odps table.
        .map(_.asInstanceOf[Partition])
    } else {
      val partitionSpecs = requiredPartition.split(",")
        .filter(!StringUtils.isBlank(_))
        .map { spec =>
          val numRecords = options.odpsUtil.getRecordCount(project, table, spec)
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
            getPartition(idx, 0, numRecords, spec).asInstanceOf[Partition]
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
            getPartition(idx, start, end - start, spec).asInstanceOf[Partition]
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

  private def getPartition(
      index: Int, start: Long, count: Long, spec: String = null): OdpsPartition = {
    OdpsPartition(this.id, index, start, count, options.accessKeyId, options.accessKeySecret,
      options.odpsUrl, options.tunnelUrl, project, table, spec)
  }

  override def checkpoint(): Unit = {
    // Do nothing. ODPS RDD should not be checkpointed.
  }

}
