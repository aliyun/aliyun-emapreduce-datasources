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
package com.aliyun.emr.examples.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkConf

object TestLoghub {
  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      System.err.println(
        """Usage: TestLoghub <sls project> <sls logstore> <loghub group name> <instance name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds>
        """.stripMargin)
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStream = args(1)
    val loghubGroupName = args(2)
    val instanceName = args(3)
    val endpoint = args(4)
    val accesskeyId = args(5)
    val accessKeySecret = args(6)
    val batchInterval = Milliseconds(args(7).toInt * 1000)

    val conf = new SparkConf().setAppName("Test SLS Loghub")
    val ssc = new StreamingContext(conf, batchInterval)
    val loghubStream = LoghubUtils.createStream(
      ssc,
      loghubProject,
      logStream,
      loghubGroupName,
      endpoint,
      accesskeyId,
      accessKeySecret,
      StorageLevel.MEMORY_AND_DISK)

    loghubStream.foreachRDD(rdd => println(rdd.count()))

    ssc.start()
    ssc.awaitTermination()
  }
}
