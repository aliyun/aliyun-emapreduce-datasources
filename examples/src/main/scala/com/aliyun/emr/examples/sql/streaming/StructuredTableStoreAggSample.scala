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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredTableStoreAggSample extends Logging {
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

    // scalastyle:off
    System.out.println(args.toSeq.toString)
    // scalastyle:on

    val checkpointLocation =
      if (args.length > 7) args(7) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("StructuredTableStoreAggSample")
      .master("local[16]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val lines = spark.readStream
      .format("tablestore")
      .option("instance.name", instanceName)
      .option("table.name", tableName)
      .option("tunnel.id", tunnelId)
      .option("endpoint", endpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("maxOffsetsPerChannel", maxOffsetsPerChannel) // default 10000
      // scalastyle:off
      .option(
        "catalog",
        """{"columns": {"UserId": {"col": "UserId", "type": "string"}, "OrderId": {"col": "OrderId", "type": "string"},
          |"price": {"cols": "price", "type": "double"}, "timestamp": {"cols": "timestamp", "type": "long"}}}""".stripMargin)
      // scalastyle:on
      .load()
      .groupBy(window(to_timestamp(col("timestamp") / 1000), "30 seconds"))
      .agg(count("*") as "count", sum("price") as "totalPrice")
      .select("window.start", "window.end", "count", "totalPrice")
      .orderBy("window")

    val query = lines.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", value = false)
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}

