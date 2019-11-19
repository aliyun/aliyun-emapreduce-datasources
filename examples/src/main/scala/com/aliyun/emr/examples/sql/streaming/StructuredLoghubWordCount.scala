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
import org.apache.spark.sql.types._

object StructuredLoghubWordCount {
  def main(args: Array[String]) {
    if (args.length < 8) {
      // scalastyle:off
      System.err.println("Usage: StructuredLoghubWordCount <logService-project> " +
        "<logService-store-in> <logService-store-out> <access-key-id> <access-key-secret> <endpoint> " +
        "<starting-offsets> <max-offsets-per-trigger> [<dynamic-config-enable> <zookeeper-connect> <checkpoint-location>]")
      // scalastyle:on
      System.exit(1)
    }

    val Array(project, logStoreIn, logStoreOut, accessKeyId, accessKeySecret, endpoint,
      startingOffsets, maxOffsetsPerTrigger, _*) = args
    val dynamicConfigEnable = if (args.length > 8) args(8) else "false"
    val zkConnect = if (args.length > 9) args(9) else "localhost:2181"
    val checkpointLocation = if (args.length > 10) {
      args(10)
    } else {
      "/tmp/temporary-" + UUID.randomUUID.toString
    }

    val spark = SparkSession
      .builder
      .appName("StructuredLoghubWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataSet representing the stream of input lines from loghub
    val schema = new StructType(
      Array(
        new StructField("__shard__", IntegerType),
        new StructField("__time__", TimestampType),
        new StructField("content", StringType)))
    val lines = spark
      .readStream
      .format("loghub")
      .schema(schema)
      .option("sls.project", project)
      .option("sls.store", logStoreIn)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("endpoint", endpoint)
      .option("dynamicConfigEnable", dynamicConfigEnable)
      .option("zookeeper.connect", zkConnect)
      .option("startingoffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select("content")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("loghub")
      .option("sls.project", project)
      .option("sls.store", logStoreOut)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("endpoint", endpoint)
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
