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

package org.apache.spark.sql.druid

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

/**
 * @date 2019/06/12
 */
object Test {
  def main(args: Array[String]): Unit = {
    val Array(curator, indexService, dataSource, firehouse, metricsSpec, dimensions, granularities, _*) = args

    val checkpoint = if (args.length > 7) {
      args(7)
    } else {
      "/tmp/streaming/druid/sink/checkpoint"
    }
    println(s"args: ${args.toList}")
    val spark = SparkSession
      .builder()
      .appName("test").master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val data = spark.readStream.format("rate").load().map(row => {
      Value(new Timestamp((System.currentTimeMillis()/1000).toLong), "value")
    }).toDF()
    val query = data
      .writeStream
      .format("druid")
      .option("curator.connect", curator)
      .option("index.service", indexService)
      .option("data.source", dataSource)
      .option("firehouse", firehouse)
      .option("metricsSpec", metricsSpec)
      .option("rollup.dimensions", dimensions)
      .option("rollup.query.granularities", granularities)
      .option("curator.max.tuning.segment.granularity", "hour")
      .option("curator.max.tuning.tuning.window.period", "PT10M")
      .option("curator.max.tuning.column", "timestamp")
      .option("checkpointLocation", checkpoint)
      .option("curator.max.tuning.timestamp.format", "posix")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}

case class Value(timestamp: Timestamp, value: String)
