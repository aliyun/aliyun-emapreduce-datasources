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
package com.aliyun.emr.examples.sql.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.UUID;

public class JavaContinuousStructuredLoghubSample {

  public static void main(String[] args) throws Exception {
    if (args.length < 7) {
      System.err.println("Usage: JavaContinuousStructuredLoghubSample <logService-project> " +
          "<logService-store> <access-key-id> <access-key-secret> " +
          "<endpoint> <starting-offsets> <max-offsets-per-trigger> [<checkpoint-location>]");
      System.exit(1);
    }

    String logProject = args[0];
    String logStore = args[1];
    String accessKeyId = args[2];
    String accessKeySecret = args[3];
    String endpoint = args[4];
    String startingOffsets = args[5];
    String maxOffsetsPerTrigger = args[6];
    String checkpointLocation = "/tmp/temporary-" + UUID.randomUUID().toString();
    if (args.length > 7) {
      checkpointLocation = args[7];
    }

    SparkSession spark = SparkSession
        .builder()
        .master("local[5]")
        .appName("JavaContinuousStructuredLoghubSample")
        .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    Dataset<String> lines = spark.readStream()
        .format("loghub")
        .option("sls.project", logProject)
        .option("sls.store", logStore)
        .option("access.key.id", accessKeyId)
        .option("access.key.secret", accessKeySecret)
        .option("endpoint", endpoint)
        .option("startingoffsets", startingOffsets)
        .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as(Encoders.STRING());

    // Start running the query that prints the running counts to the console
    StreamingQuery query = lines.writeStream()
        .outputMode("append")
        .format("console")
        .option("checkpointLocation", checkpointLocation)
        .trigger(Trigger.Continuous("5 second"))
        .start();

    query.awaitTermination();
  }
}
