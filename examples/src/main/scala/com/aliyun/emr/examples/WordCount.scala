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

package com.aliyun.emr.examples

import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.Record

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.aliyun.odps.OdpsOps
import org.apache.spark.rdd.RDD

/** Counts words in new text files created in the given directory */
object WordCount {
  def main(args: Array[String]): Unit = {
    val inputType = args(0)
    val conf = new SparkConf().setAppName("WordCount")

    val inputRDD: RDD[String] = inputType match {
      case "odps" =>
        if (args.length < 8) {
          // scalastyle:off
          System.err.println("Usage: WordCount <inputType> <project> <table> " +
            "<numPartitions> <accessKeyId> <accessKeySecret> <odpsUrl> <tunnelUrl>")
          // scalastyle:on
          System.exit(1)
        }
        val project = args(1)
        val table = args(2)
        val numPartition = args(3).toInt
        val accessKeyId = args(4)
        val accessKeySecret = args(5)
        val odpsUrl = args(6)
        val tunnelUrl = args(7)
        val sc = new SparkContext(conf)
        OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
          .readTable(project, table, read0, numPartition)

      case "oss" =>
        if (args.length < 6) {
          // scalastyle:off
          System.err.println("Usage: WordCount <inputType> <inputPath> <numPartitions> " +
            "<accessKeyId> <accessKeySecret> <endpoint>")
          // scalastyle:on
          System.exit(1)
        }
        val inputPath = args(1)
        val numPartition = args(2).toInt
        val accessKeyId = args(3)
        val accessKeySecret = args(4)
        val endpoint = args(5)
        conf.set("spark.hadoop.fs.oss.accessKeyId", accessKeyId)
        conf.set("spark.hadoop.fs.oss.accessKeySecret", accessKeySecret)
        conf.set("spark.hadoop.fs.oss.endpoint", endpoint)
        val sc = new SparkContext(conf)
        sc.textFile(inputPath, numPartition)

      case "hdfs" =>
        if (args.length < 3) {
          // scalastyle:off
          System.err.println("Usage: WordCount <inputType> <inputPath> <numPartitions>")
          // scalastyle:on
          System.exit(1)
        }

        val inputPath = args(1)
        val numPartitions = args(2).toInt
        val sc = new SparkContext(conf)
        sc.textFile(inputPath, numPartitions)
    }

    inputRDD.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
      // scalastyle:off
      .collect().foreach(println)
    // scalastyle:on
  }

  def read0(record: Record, schema: TableSchema): String = {
    record.getString(0)
  }
}
