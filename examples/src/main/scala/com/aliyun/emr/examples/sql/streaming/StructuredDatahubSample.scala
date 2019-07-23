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
package com.aliyun.emr.examples.sql.streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredDatahubSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 8) {
      println(
        """
          |Usage: Usage: StructuredDatahubSample <endpoint> <project> <topic> <access key id>
          |       <access key secret> <zookeeper host:port> <max offset per trigger>
          |       [checkpoint directory=/tmp/datahub/test/checkpoint]
          |
        """.stripMargin)
      sys.exit(1)
    }

    val Array(endpoint, project, topic, accessKeyId, accessKeySecret, zkHosts, maxOffset, triggerInterval, _*) = args
    val checkpointDir = if (args.length > 8) {
      args(8)
    } else {
      "/tmp/temporary-" + UUID.randomUUID.toString
    }

    val spark = SparkSession.builder()
      .appName("StructuredDatahubSample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val value = spark.readStream.format("datahub")
      .option("endpoint", endpoint)
      .option("project", project)
      .option("topic", topic)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("max.offset.per.trigger", maxOffset)
      .option("zookeeper.connect.address", zkHosts)
      .option("decimal.precision", "5")
      .option("decimal.scale", "5")
      .load()

    val query = value.select("*").writeStream.format("console")
      .option("checkpointLocation", checkpointDir)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerInterval.toLong))
      .start()
    query.awaitTermination()
  }
}
