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

object StructuredTableStoreSinkSample {
  def main(args: Array[String]) {
    if (args.length < 8) {
      System.err.println("Usage: StructuredTableStoreSinkSample <logService-project> " +
        "<logService-store> <access-key-id> <access-key-secret> <sls-endpoint> <ots-endpoint> " +
        "<ots-table-name> <ots-instance-name> <starting-offsets> <max-offsets-per-trigger> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(project, logStore, accessKeyId, accessKeySecret, slsEndpoint, otsEndpoint, otsTableName, otsInstanceName,
      startingOffsets, maxOffsetsPerTrigger, _*) = args
    val checkpointLocation =
      if (args.length > 8) args(8) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("StructuredTableStoreSinkSample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Create DataSet representing the stream of input lines from loghub
    val lines = spark
      .readStream
      .format("loghub")
      .option("sls.project", project)
      .option("sls.store", logStore)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("endpoint", slsEndpoint)
      .option("startingoffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .selectExpr("CAST(content AS STRING)")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("tablestore")
      .option("endpoint", otsEndpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("table.name", otsTableName)
      .option("instance.name", otsInstanceName)
      .option("batch.update.size", "100")
      .option("catalog", """{"columns":{"value":{"col":"value","type":"string"},"count":{"col":"count","type":"long"}}}""")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
