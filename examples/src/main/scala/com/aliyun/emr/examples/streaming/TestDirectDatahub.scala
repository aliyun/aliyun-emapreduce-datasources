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

import com.aliyun.datahub.model.RecordEntry
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.aliyun.datahub.{CanCommitOffsets, DatahubUtils, DirectDatahubInputDStream}

object TestDirectDatahub {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      println(
        """
          |Usage: TestDirectDatahub endpoint project topic subId accessId accessKey duration [zookeeper-host:port]
        """.stripMargin)
      sys.exit(1)
    }
    val endpoint = args(0)
    val project = args(1)
    val topic = args(2)
    val subId = args(3)
    val accessId = args(4)
    val accessKey = args(5)
    val duration = args(6).toLong * 1000
    val zkServer = if (args.length > 7 ) args(7) else "localhost:2181"

    val checkpointDir = "/tmp/datahub/checkpoint/test-direct-datahub"
    val zkParam = Map("zookeeper.connect" -> zkServer)

    def getStreamingContext(): StreamingContext = {
      val sc = new SparkContext(new SparkConf().setAppName("test-direct-datahub"))
      sc.setLogLevel("ERROR")
      val ssc = new StreamingContext(sc, Duration(duration))
      val dstream = DatahubUtils.createDirectStream(
        ssc,
        endpoint,
        project,
        topic,
        subId,
        accessId,
        accessKey,
        read(_),
        zkParam)

      dstream.checkpoint(Duration(duration)).foreachRDD(rdd => {
        println(s"count:${rdd.count()}")
        dstream.asInstanceOf[CanCommitOffsets].commitAsync()
      })
      ssc.checkpoint(checkpointDir)
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDir, getStreamingContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def read(recordEntry: RecordEntry): String = {
    recordEntry.toJsonNode.toString
  }

}
