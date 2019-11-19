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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger


object ContinuousStructuredDatahubSample extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      // scalastyle:off
      println(
        """
          |Usage: <endpoint> <project> <topic> <zk host:port>
          |       <access key id> <access key secret> [checkpoint directory]
        """.stripMargin)
      // scalastyle:on
      sys.exit(1)
    }
    val Array(endpoint, project, topic, zkHost, accessKeyId, accessKeySecret, _*) = args
    val checkpoint = if (args.length > 5) args(5) else "/tmp/datahub/test/checkpoint"

    val spark = SparkSession.builder().appName("datahub sample").getOrCreate()

    import spark.implicits._
    val value = spark.readStream
      .format("datahub")
      .option("project", project)
      .option("endpoint", endpoint)
      .option("topic", topic)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("zookeeper.connect.address", zkHost)
      .load()
      .select("value0", "value1").as[Value]
      .map(r => {
        (r.value0, r.value0.length, r.value1, r.value1.length)
      }).toDF("value0", "len0", "value1", "len1")

    val query = value.writeStream
      .format("console")
      .option("checkpointLocation", checkpoint)
      .outputMode("append")
      .trigger(Trigger.Continuous(1000))
      .start()

    query.awaitTermination()
  }

  case class Value(value0: String, value1: String)
}
