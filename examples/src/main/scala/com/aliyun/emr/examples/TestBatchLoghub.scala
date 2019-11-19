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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils

object TestBatchLoghub {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      // scalastyle:off
      System.err.println(
        """Usage: TestBatchLoghub <sls project> <sls logstore> <sls endpoint>
          |         <access key id> <access key secret> <start time> <end time=now>
        """.stripMargin)
      // scalastyle:on
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val endpoint = args(2)
    val accessKeyId = args(3)
    val accessKeySecret = args(4)
    val startTime = args(5).toLong

    val sc = new SparkContext(new SparkConf().setAppName("test batch loghub"))
    var rdd: JavaRDD[String] = null
    if (args.length > 6) {
      rdd = LoghubUtils.createRDD(sc, loghubProject, logStore, accessKeyId, accessKeySecret,
        endpoint, startTime, args(6).toLong)
    } else {
      rdd = LoghubUtils.createRDD(sc, loghubProject, logStore, accessKeyId, accessKeySecret,
        endpoint, startTime)
    }

    // scalastyle:off
    println("get log count:" + rdd.count())
    // scalastyle:on
  }
}
