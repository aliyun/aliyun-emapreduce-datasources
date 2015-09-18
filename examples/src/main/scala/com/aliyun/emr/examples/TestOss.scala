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

package com.aliyun.emr.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.aliyun.oss.OssOps

object TestOss {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(
        """Usage: TestOss <accessKeyId> <accessKeySecret> <endpoint> <inputPath> <numPartition>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    endpoint         Aliyun OSS endpoint.
          |    inputPath        Aliyun OSS object path, supporting two modes: block-based and native.
          |                     block-based: ossbfs://bucket/object/path
          |                     native: oss://bucket/object/path
          |    numPartitions    the number of RDD partitions.
          |
        """.stripMargin)
    }

    val accessKeyId = args(0)
    val accessKeySecret = args(1)
    val endpoint = args(2)
    val inputPath = args(3)
    val numPartitions = args(4).toInt

    val conf = new SparkConf().setAppName("Test OSS Read")
    val sc = new SparkContext(conf)

    val ossData = OssOps(sc, endpoint, accessKeyId, accessKeySecret).readOssFile(inputPath, numPartitions)
    println("The top 10 lines are:")
    ossData.top(10).foreach(println)
  }
}
