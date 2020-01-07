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

object StructuredTableStoreWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      // scalastyle:off
      System.err.println(
        "Usage: StructuredTableStoreWordCount <ots-instanceName>" +
          "<ots-tableName> <ots-tunnelId> <access-key-id> <access-key-secret> <ots-endpoint>" +
          "<max-offsets-per-channel> [<checkpoint-location>]")
      // scalastyle:on
    }

    val Array(
      instanceName,
      tableName,
      tunnelId,
      accessKeyId,
      accessKeySecret,
      endpoint,
      maxOffsetsPerChannel,
      _*
    ) = args

    val checkpointLocation =
      if (args.length > 7) args(7)
      else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession.builder
      .appName("TableStoreWordCount")
      .master("local[5]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.readStream
      .format("tablestore")
      .option("instance.name", instanceName)
      .option("table.name", tableName)
      .option("tunnel.id", tunnelId)
      .option("endpoint", endpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("maxOffsetsPerChannel", maxOffsetsPerChannel)
      // scalastyle:off
      .option(
        "catalog",
        "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"int\"}," +
          "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}}}")
      // scalastyle:on
      .load()
      .selectExpr("PkString", "PkInt", "__ots_record_type__")
      .as[(String, Integer, String)]

    // scalastyle:off
    println(lines.printSchema())
    // scalastyle:on
    val wordCounts = lines
      .flatMap(line => {
        // scalastyle:off
        System.out.println(s"wordCount line: $line")
        // scalastyle:on
        line._1.split(" ")
      })
      .groupBy("value")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
