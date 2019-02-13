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

object ContinuousStructuredLoghubSample {
  def main(args: Array[String]) {
    if (args.length < 6) {
      System.err.println("Usage: ContinuousStructuredLoghubSample <logService-project> " +
        "<logService-store> <access-key-id> <access-key-secret> <endpoint> " +
        "<starting-offsets> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(project, logStore, accessKeyId, accessKeySecret, endpoint, startingOffsets, _*) = args
    val checkpointLocation =
      if (args.length > 6) args(6) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("ContinuousStructuredLoghubSample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataSet representing the stream of input lines from loghub
    val lineLength = spark
      .readStream
      .format("loghub")
      .option("sls.project", project)
      .option("sls.store", logStore)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("endpoint", endpoint)
      .option("startingoffsets", startingOffsets)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String].map(e => (e, e.length)).toDF("value", "length")

    val query = lineLength.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.Continuous("5 second"))
      .start()

    query.awaitTermination()
  }
}
