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

object StructuredKuduSinkSample {
  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println("Usage: StructuredKuduSinkSample <logService-project> <logService-store> " +
        "<access-key-id> <access-key-secret> <endpoint> <starting-offsets> <max-offsets-per-trigger> " +
        "<kudu-master-url> <kudu-table> [<checkpoint-location>]")
      System.exit(1)
    }

    val Array(project, logStore, accessKeyId, accessKeySecret, endpoint, startingOffsets,
      kuduMasterUrl, kuduTableName, maxOffsetsPerTrigger, _*) = args
    val checkpointLocation =
      if (args.length > 9) args(9) else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession
      .builder
      .appName("StructuredKuduSinkSample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark
      .readStream
      .format("loghub")
      .option("sls.project", project)
      .option("sls.store", logStore)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("endpoint", endpoint)
      .option("startingoffsets", startingOffsets)
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()
      .select("__value__")
      .as[String]

    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("kudu")
      .option("kudu.master", kuduMasterUrl)
      .option("kudu.table", kuduTableName)
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
