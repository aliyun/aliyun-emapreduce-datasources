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
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.datahub.DatahubUtils
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object TestDatahub {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: TestDatahub <project> <topic> <subscribe Id> <access key id>
          |         <access key secret> <endpoint> <batch interval seconds> [<shard Id>]
        """.stripMargin)
      System.exit(1)
    }

    var isShardDefined = false
    if (args.length == 8) {
      isShardDefined = true
    }

    val project = args(0)
    val topic = args(1)
    val subId = args(2)
    val accessKeyId = args(3)
    val accessKeySecret = args(4)
    val endpoint = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("Test Datahub")
      val ssc = new StreamingContext(conf, batchInterval)
      var datahubStream: DStream[Array[Byte]] = null
      if (isShardDefined){
        val shardId = args(7)
        datahubStream = DatahubUtils.createStream(
          ssc,
          project,
          topic,
          subId,
          accessKeyId,
          accessKeySecret,
          endpoint,
          shardId,
          read(_),
          StorageLevel.MEMORY_AND_DISK)
      } else {
        datahubStream = DatahubUtils.createStream(
          ssc,
          project,
          topic,
          subId,
          accessKeyId,
          accessKeySecret,
          endpoint,
          read(_),
          StorageLevel.MEMORY_AND_DISK)
      }

      datahubStream.checkpoint(batchInterval * 2).foreachRDD(rdd => println(rdd.count()))
      ssc.checkpoint("hdfs:///tmp/spark/streaming") // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate("hdfs:///tmp/spark/streaming", functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }

  def read(record: RecordEntry): String = {
    record.getString(0)
  }
}
