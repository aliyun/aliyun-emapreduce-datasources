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

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.common.LogGroupData
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object TestDirectLoghubCustomDecoder {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: TestDirectLoghub <sls project> <sls logstore> <loghub group name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds> <zookeeper host:port=localhost:2181>
        """.stripMargin)
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val loghubGroupName = args(2)
    val endpoint = args(3)
    val accessKeyId = args(4)
    val accessKeySecret = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)
    val zkAddress = if (args.length >= 8) args(7) else "localhost:2181"


    def myDecoder(logGroup: LogGroupData): ArrayBuffer[String] = {
      val fastLogGroup = logGroup.GetFastLogGroup()
      val logCount = fastLogGroup.getLogsCount
      val result = new ArrayBuffer[String](logCount)
      for (i <- 0 until logCount) {
        val log = fastLogGroup.getLogs(i)
        val fieldCount = log.getContentsCount
        val obj = new JSONObject(fieldCount)
        for (j <- 0 until log.getContentsCount) {
          val field = log.getContents(j)
          obj.put(field.getKey, field.getValue)
        }
        result += obj.toJSONString
      }
      result
    }

    val conf = new SparkConf().setMaster("local[2]").setAppName("Test Direct SLS Loghub")
      .set("enable.auto.commit", "false")
    val ssc = new StreamingContext(conf, batchInterval)
    val zkParas = Map("zookeeper.connect" -> zkAddress,
      "enable.auto.commit" -> "false")
    val loghubStream = LoghubUtils.createDirectStream(
      ssc,
      loghubProject,
      logStore,
      loghubGroupName,
      accessKeyId,
      accessKeySecret,
      endpoint,
      zkParas,
      LogHubCursorPosition.END_CURSOR,
      -1,
      myDecoder _
    )

    loghubStream.print(10)
    ssc.checkpoint("hdfs:///tmp/spark/streaming") // set checkpoint directory

    ssc.start()
    ssc.awaitTermination()
  }
}
