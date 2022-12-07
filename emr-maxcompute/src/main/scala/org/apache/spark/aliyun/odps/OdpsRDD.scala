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
package org.apache.spark.aliyun.odps

import java.io.EOFException

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.aliyun.odps.{Odps, PartitionSpec, TableSchema}
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.Record
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelRecordReader

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{NextIterator, TaskCompletionListener}

case class OdpsPartition(rddId: Int,
    idx: Int,
    start: Long,
    count: Long,
    accessKeyId: String,
    accessKeySecret: String,
    odpsUrl: String,
    tunnelUrl: String,
    project: String,
    table: String,
    part: String)
  extends Partition {

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override def equals(other: Any): Boolean = super.equals(other)

  override val index: Int = idx
}

class OdpsRDD[T: ClassTag](@transient sc: SparkContext,
    accessKeyId: String, accessKeySecret: String, odpsUrl: String, tunnelUrl: String,
    project: String, table: String, part: String,
    numPartition: Int,
    transfer: (Record, TableSchema) => T)
  extends RDD[T](sc, Nil) with Logging {

  def this(sc: SparkContext, accessKeyId: String, accessKeySecret: String,
    odpsUrl: String, tunnelUrl: String,
    project: String, table: String,
    numPartition: Int,
    transfer: (Record, TableSchema) => T) {

    this(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl, project, table,
      "Non-Partitioned", numPartition, transfer)
  }

  /** Implemented by subclasses to compute a given partition. */
  override def compute(theSplit: Partition, context: TaskContext): Iterator[T] = {
    val iter = new NextIterator[T] {
      val split = theSplit.asInstanceOf[OdpsPartition]

      val account = new AliyunAccount(accessKeyId, accessKeySecret)
      val odps = new Odps(account)
      odps.setDefaultProject(project)
      odps.setEndpoint(odpsUrl)
      val tunnel = new TableTunnel(odps)
      tunnel.setEndpoint(tunnelUrl)
      var downloadSession: TableTunnel#DownloadSession = null
      if(part.equals("Non-Partitioned")) {
        downloadSession = tunnel.createDownloadSession(project, table)
      } else {
        val partitionSpec = new PartitionSpec(part)
        downloadSession = tunnel.createDownloadSession(project, table, partitionSpec)
      }
      val reader = downloadSession.openRecordReader(split.start, split.count)
      val inputMetrics = context.taskMetrics.inputMetrics

      context.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          closeIfNeeded()
        }
      })

      override def getNext() = {
        var ret = null.asInstanceOf[T]
        try {
          val r = reader.read()
          if (r != null) {
            ret = transfer(r, downloadSession.getSchema)
            inputMetrics.incRecordsRead(1L)
          } else {
            finished = true
          }
        } catch {
          case eof: EOFException =>
            finished = true
        }
        ret
      }

      override def close() {
        try {
          val totalBytes = reader.asInstanceOf[TunnelRecordReader].getTotalBytes
          inputMetrics.incBytesRead(totalBytes)
          reader.close()
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }

    new InterruptibleIterator[T](context, iter)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = {
    var ret = null.asInstanceOf[Array[Partition]]

    val account = new AliyunAccount(accessKeyId, accessKeySecret)
    val odps = new Odps(account)
    odps.setDefaultProject(project)
    odps.setEndpoint(odpsUrl)
    val tunnel = new TableTunnel(odps)
    tunnel.setEndpoint(tunnelUrl)
    var downloadSession: TableTunnel#DownloadSession = null
    if(part.equals("Non-Partitioned")) {
      downloadSession = tunnel.createDownloadSession(project, table)
    } else {
      val partitionSpec = new PartitionSpec(part)
      downloadSession = tunnel.createDownloadSession(project, table, partitionSpec)
    }
    val downloadCount = downloadSession.getRecordCount
    logDebug("Odps project " + project + " table " + table + " with partition "
      + part + " contain " + downloadCount + " line data.")
    var numPartition_ = math.min(math.max(1, numPartition),
      (if (downloadCount > Int.MaxValue) Int.MaxValue else downloadCount.toInt))
    if(numPartition_ == 0) {
      numPartition_ = 1
      logDebug("OdpsRdd has one partition at least.")
    }
    val range = getRanges(downloadCount, 0, numPartition_)
    ret = Array.tabulate(numPartition_) {
      idx =>
        val (start, end) = range(idx)
        val count = (end - start + 1).toInt
        new OdpsPartition(
          this.id,
          idx,
          start,
          count,
          accessKeyId,
          accessKeySecret,
          odpsUrl,
          tunnelUrl,
          project,
          table,
          part
        )
    }.filter(p => p.count > 0)
      // remove the last count==0 to prevent exceptions from reading odps table.
      .map(_.asInstanceOf[Partition])
    ret
  }

  def getRanges(max: Long, min: Long, numRanges: Int): Array[(Long, Long)] = {
    val span = max - min + 1
    val initSize = span / numRanges
    val sizes = Array.fill(numRanges)(initSize)
    val remainder = span - numRanges * initSize
    for (i <- 0 until remainder.toInt) {
      sizes(i) += 1
    }
    assert(sizes.reduce(_ + _) == span)
    val ranges = ArrayBuffer.empty[(Long, Long)]
    var start = min
    sizes.filter(_ > 0).foreach { size =>
      val end = start + size - 1
      ranges += Tuple2(start, end)
      start = end + 1
    }
    assert(start == max + 1)
    ranges.toArray
  }


  override def checkpoint() {
    // Do nothing. ODPS RDD should not be checkpointed.
  }
}
