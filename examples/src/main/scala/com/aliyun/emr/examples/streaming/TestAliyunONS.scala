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

import java.util.{Properties, UUID}

import com.aliyun.openservices.ons.api.{Message, PropertyKeyConst}
import com.aliyun.openservices.ons.api.impl.ONSFactoryImpl

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.aliyun.ons.OnsUtils

object TestOnsStreaming {
  def main(args: Array[String]): Unit = {
    val Array(accessKeyId, accessKeySecret, cId, topic, subExpression, parallelism) = args

    val numStreams = parallelism.toInt
    val batchInterval = Milliseconds(2000)

    val conf = new SparkConf().setAppName("Test ONS Streaming")
    val ssc = new StreamingContext(conf, batchInterval)
    def func: Message => Array[Byte] = msg => msg.getBody
    val onsStreams = (0 until numStreams).map { i =>
      // scalastyle:off
      println(s"starting stream $i")
      // scalastyle:on
      OnsUtils.createStream(
        ssc,
        cId,
        topic,
        subExpression,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK_2,
        func)
    }

    val unionStreams = ssc.union(onsStreams)
    // scalastyle:off
    unionStreams.foreachRDD(rdd => println(s"count: ${rdd.count()}"))
    // scalastyle:on

    ssc.start()
    ssc.awaitTermination()
  }
}

object OnsRecordProducer {
  def main(args: Array[String]): Unit = {
    val Array(accessKeyId, accessKeySecret, pId, topic, tag, parallelism) = args

    val numPartition = parallelism.toInt
    val conf = new SparkConf().setAppName("ONS Record Producer")
    val sc = new SparkContext(conf)

    sc.parallelize(0 until numPartition, numPartition).mapPartitionsWithIndex {
      (index, itr) => {
        generate(index, accessKeyId, accessKeySecret, pId, topic, tag)
        Iterator.empty
      }
    }.count()
  }

  def generate(
      partitionId: Int,
      accessKeyId: String,
      accessKeySecret: String,
      pId: String,
      topic: String,
      tag: String): Unit = {
    val properties = new Properties()
    properties.put(PropertyKeyConst.ProducerId, pId)
    properties.put(PropertyKeyConst.AccessKey, accessKeyId)
    properties.put(PropertyKeyConst.SecretKey, accessKeySecret)
    val onsFactoryImpl = new ONSFactoryImpl
    val producer = onsFactoryImpl.createProducer(properties)
    producer.shutdown()
    producer.start()

    var count = 0
    while(true) {
      val uuid = UUID.randomUUID()
      val msg = new Message(topic, tag, uuid.toString.getBytes)
      msg.setKey(s"ORDERID_${partitionId}_$count")
      producer.send(msg)
      count += 1
      Thread.sleep(100L)
    }
  }
}
