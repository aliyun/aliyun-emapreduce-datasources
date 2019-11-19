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

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils

object TestLoghub {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      // scalastyle:off
      System.err.println(
        """Usage: TestLoghub <sls project> <sls logstore> <loghub group name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds>
        """.stripMargin)
      // scalastyle:on
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val loghubGroupName = args(2)
    val endpoint = args(3)
    val accessKeyId = args(4)
    val accessKeySecret = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("Test SLS Loghub")
      val ssc = new StreamingContext(conf, batchInterval)
      val loghubStream = LoghubUtils.createStream(
        ssc,
        loghubProject,
        logStore,
        loghubGroupName,
        endpoint,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK)

      loghubStream.checkpoint(batchInterval * 2).foreachRDD(rdd =>
        // scalastyle:off
        rdd.map(bytes => new String(bytes)).top(10).foreach(println)
        // scalastyle:on
      )
      ssc.checkpoint("hdfs:///tmp/spark/streaming") // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate("hdfs:///tmp/spark/streaming", functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
