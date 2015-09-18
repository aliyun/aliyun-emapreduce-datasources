/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.aliyun.oss

import java.io.EOFException

import com.aliyun.fs.utils.OssInputUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.FileSplit
import org.apache.spark._
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.rdd.RDD
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag

class OssPartition(
    rddId: Int,
    idx: Int,
    @transient split: FileSplit)
  extends Partition {
  val inputSplit = new SerializableWritable[FileSplit](split)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx
}
class OssRDD(
    @transient sc: SparkContext,
    path: String,
    numPartitions: Int,
    endpoint: String,
    accessKeyId: String,
    accessKeySecret: String,
    securityToken: Option[String] = None)
  extends RDD[String](sc, Nil) with Logging {

  @transient private val sparkConf = sc.getConf
  @transient private val hadoopConfiguration: Configuration = {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.oss.endpoint", endpoint)
    hadoopConf.set("fs.oss.accessKeyId", accessKeyId)
    hadoopConf.set("fs.oss.accessKeySecret", accessKeySecret)
    hadoopConf.set("fs.oss.securityToken", securityToken.getOrElse("null"))
    hadoopConf.set("fs.ossn.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
    hadoopConf.set("fs.oss.impl", "com.aliyun.fs.oss.blk.OssFileSystem")

    if (sparkConf != null) {
      sparkConf.getAll.foreach{ case (key, value) =>
        if (key.startsWith("spark.hadoop.")) {
          hadoopConf.set(key.substring("spark.hadoop.".length), value)
        }
      }

      val bufferSize = sparkConf.get("spark.buffer.size", "65536")
      hadoopConf.set("io.file.buffer.size", bufferSize)
    }

    hadoopConf
  }

  val serializableHadoopConf = new SerializableWritable[Configuration](hadoopConfiguration)

  /** Implemented by subclasses to compute a given partition. */
  override def compute(theSplit: Partition, context: TaskContext): Iterator[String] = {
    val conf = serializableHadoopConf.value
    val iter = new NextIterator[String] {
      val split = theSplit.asInstanceOf[OssPartition]
      logInfo("Input split: " + split.inputSplit)
      val ossInputUtils = new OssInputUtils(conf)
      val reader = ossInputUtils.getOssRecordReader(split.inputSplit.value, conf)
      val inputMetrics = context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop)

      val key: LongWritable = reader.createKey()
      val value: Text = reader.createValue()
      override def getNext() = {
        var ret: String = ""
        try {
          finished = !reader.next(key, value)
          if (!finished) {
            ret = value.toString
            inputMetrics.incRecordsRead(1L)
          }
        } catch {
          case eof: EOFException =>
            finished = true
        }

        ret
      }

      override def close() {
        try {
          reader.close()
          try {
            inputMetrics.incBytesRead(split.inputSplit.value.getLength)
          } catch  {
            case e: java.io.IOException =>
              logWarning("Unable to get input size to set InputMetrics for task", e)
          }
        } catch {
          case e: Exception => logWarning("Exception in RecordReader.close()", e)
        }
      }
    }

    new InterruptibleIterator[String](context, iter)
  }

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
  override def getPartitions: Array[Partition] = {
    val ossInputUtils = new OssInputUtils(hadoopConfiguration)
    val splits = ossInputUtils.getSplits(path, numPartitions)
    Array.tabulate(splits.length) {
      idx =>
        val split = splits(idx)
        new OssPartition(
          this.id,
          idx,
          split
        )
    }.map(_.asInstanceOf[Partition])
  }

  override def checkpoint() {
    // Do nothing.
  }
}

