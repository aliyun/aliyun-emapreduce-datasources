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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.executor.{DataWriteMethod, OutputMetrics}
import org.apache.spark.rdd.RDD

class OssOps(
    @transient sc: SparkContext,
    endpoint: String,
    accessKeyId: String,
    accessKeySecret: String,
    securityToken: Option[String] = None)
  extends Logging with Serializable {

  /**
   * Read data from OSS.
   *
   * {{{
   *   test
   * }}}
   *
   * @param path A OSS file path which job is reading.
   * @param minPartitions l
   * @return
   */
  def readOssFileWithJava(
      path: String,
      minPartitions: Int): JavaRDD[String] = {
    new JavaRDD(readOssFile(path, minPartitions))
  }

  def saveToOssFileWithJava[T](
      path: String,
      javaRdd: JavaRDD[T]): Unit = {
    saveToOssFile(path, javaRdd.rdd)
  }

  def readOssFile(
      path: String,
      minPartitions: Int): RDD[String] = {
    new OssRDD(sc, path, minPartitions, endpoint, accessKeyId, accessKeySecret, securityToken)
  }

  def saveToOssFile[T](
      path: String,
      rdd: RDD[T]): Unit = {
    val hadoopConfiguration: Configuration = {
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.oss.endpoint", endpoint)
      hadoopConf.set("fs.oss.accessKeyId", accessKeyId)
      hadoopConf.set("fs.oss.accessKeySecret", accessKeySecret)
      securityToken match {
        case Some(token) =>
          hadoopConf.set("fs.oss.securityToken", token)
        case None =>
      }
      hadoopConf.set("fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
      hadoopConf.set("fs.ossbfs.impl", "com.aliyun.fs.oss.blk.OssFileSystem")

      val sparkConf = sc.getConf
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

    val filePath = new Path(path)
    val fs = FileSystem.get(filePath.toUri, hadoopConfiguration)
    fs.initialize(filePath.toUri, hadoopConfiguration)
    // We need to delete the old file first, and then write.
    fs.delete(filePath)
    val serializedHadoopConf = new SerializableWritable[Configuration](hadoopConfiguration)
    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val conf = serializedHadoopConf.value
      val tmpPath = new Path(path + "/part-" + context.partitionId())
      val fs = FileSystem.get(tmpPath.toUri, conf)
      fs.initialize(tmpPath.toUri, conf)
      val out = fs.create(tmpPath)
      var recordsWritten = 0L
      val outputMetrics = new OutputMetrics(DataWriteMethod.Hadoop)
      context.taskMetrics.outputMetrics = Some(outputMetrics)

      while(iter.hasNext) {
        val value = iter.next()
        val line = s"${value.toString}\n"
        out.writeBytes(line)
        recordsWritten += 1
      }
      out.flush()
      out.close()
      outputMetrics.setRecordsWritten(recordsWritten)
      val byteLength = fs.getFileStatus(tmpPath).getLen
      outputMetrics.setBytesWritten(byteLength)
    }
    sc.runJob(rdd, writeToFile _)
  }
}

object OssOps {

  def apply(sc: SparkContext, endpoint: String, accessKeyId: String, accessKeySecret: String): OssOps = {
    new OssOps(sc, endpoint, accessKeyId, accessKeySecret)
  }

  def apply(sc: SparkContext, endpoint: String, accessKeyId: String, accessKeySecret: String, securityToken: String)
    : OssOps = {
    new OssOps(sc, endpoint, accessKeyId, accessKeySecret, Some(securityToken))
  }
}
