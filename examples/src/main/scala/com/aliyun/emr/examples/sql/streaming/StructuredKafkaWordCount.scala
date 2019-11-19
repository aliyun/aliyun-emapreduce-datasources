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

object StructuredKafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 3) {
      // scalastyle:off
      System.err.println("Usage: StructuredKafkaWordCount <bootstrapSevers> <topicIn> <topicOut> " +
        "[<checkpoint-location>]")
      // scalastyle:on
      System.exit(1)
    }

    val Array(bootstrapSevers, topicIn, topicOut) = args
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataSet representing the stream of input lines from loghub
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapSevers)
      .option("subscribe", topicIn)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapSevers)
      .option("topic", topicOut)
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
