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

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object StructuredLoghubSinkHive {
  def main(args: Array[String]) {
    if (args.length < 9) {
      // scalastyle:off
      System.err.println("Usage: StructuredLoghubSinkHive <logService-project> " +
        "<logService-store> <access-key-id> <access-key-secret> <endpoint> " +
        "<starting-offsets> <database> <table> <max-offsets-per-trigger> [<checkpoint-location>]")
      // scalastyle:on
      System.exit(1)
    }

    val Array(project, logStore, accessKeyId, accessKeySecret, endpoint, startingOffsets,
      db, table, maxOffsetsPerTrigger, _*) = args
    val checkpointLocation =
      if (args.length > 9) args(9) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .enableHiveSupport()
      .appName("StructuredLoghubSinkHive")
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
      .option("sls.store", logStore)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("endpoint", endpoint)
      .option("startingoffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select("content")
      .as[String]

    val query = lines.writeStream
      .outputMode("append")
      .foreachBatch { (ds: Dataset[String], epochId: Long) =>
          ds.limit(10).write.mode(SaveMode.Append).insertInto(s"$db.$table")
      }
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
