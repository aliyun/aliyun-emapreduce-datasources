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

package org.apache.spark.sql.aliyun.datahub

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.execution.streaming.sources.ContinuousMemoryStream
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamingExecutionRelation}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


abstract class DatahubSinkSuite extends QueryTest with SharedSparkSession {

  protected var testUtils: DatahubTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new DatahubTestUtils()
    testUtils.init()
  }

  override def afterAll(): Unit = {
    // testUtils.cleanAllResource()
  }

  protected def createDatahubReader(project: String, topic: String): DataFrame = {
    spark.readStream
      .format("datahub")
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_ID, testUtils.accessKeyId)
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_KEY, testUtils.accessKeySecret)
      .option(DatahubSourceProvider.OPTION_KEY_ENDPOINT, testUtils.endpoint)
      .option(DatahubSourceProvider.OPTION_KEY_PROJECT, testUtils.project)
      .option(DatahubSourceProvider.OPTION_KEY_TOPIC, topic)
      .load()
  }

  val defaultSchema = StructType(Array(StructField("id", LongType), StructField("msg", StringType)))
}

class DatahubSinkBatchSuiteBase extends DatahubSinkSuite {
  test (" simple batch write to datahub") {
    val topic = Option(System.getenv("DATAHUB_TOPIC")).getOrElse {
      testUtils.createTopic(defaultSchema)
    }
    val data = Seq(
      Row(1L, "hello spark batch"),  // .getBytes(UTF_8)
      Row(2L, "hello datahub"),
      Row(3L, "emr")
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      defaultSchema
    )

    df.write
      .format("datahub")
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_ID, testUtils.accessKeyId)
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_KEY, testUtils.accessKeySecret)
      .option(DatahubSourceProvider.OPTION_KEY_ENDPOINT, testUtils.endpoint)
      .option(DatahubSourceProvider.OPTION_KEY_PROJECT, testUtils.project)
      .option(DatahubSourceProvider.OPTION_KEY_TOPIC, topic)
      .mode("append")
      .save()
  }

}

class DatahubSinkStreamingSuiteBase extends DatahubSinkSuite {
}

class DatahubSinkMicroBatchStreamingSuite extends DatahubSinkStreamingSuiteBase {
  import testImplicits._
  ignore("microbatch -default") {
    val topic = testUtils.createTopic(defaultSchema)
    val input = MemoryStream[(Long, String)]
    input.toDF().select($"_1" as "id", $"_2" as "msg")

    val query = input
      .toDF()
      .select($"_1" as "id", $"_2" as "msg")
      .writeStream
//      .format("console").start()
      .format("datahub")
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_ID, testUtils.accessKeyId)
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_KEY, testUtils.accessKeySecret)
      .option(DatahubSourceProvider.OPTION_KEY_ENDPOINT, testUtils.endpoint)
      .option(DatahubSourceProvider.OPTION_KEY_PROJECT, testUtils.project)
      .option(DatahubSourceProvider.OPTION_KEY_TOPIC, topic)
      .option("checkpointLocation", "/tmp/spark/checkpoint")
      .start()

    try {
      input.addData((1, "msg1"), (2, "msg2"), (3, "msg3"))
      query.processAllAvailable()
      input.addData((4, "msg4"), (5, "msg5"), (6, "msg6"))
      query.processAllAvailable()
      input.addData()
      query.processAllAvailable()
    } finally {
      query.stop()
    }
  }
}

class DatahubContinuousSinkSuite extends DatahubSinkStreamingSuiteBase {
  import testImplicits._
  test("continuous - default") {
    // val topic = testUtils.createTopic(defaultSchema)
    val input = spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)

    val query = input
      .writeStream
//      .format("console")
      .format("datahub")
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_ID, testUtils.accessKeyId)
      .option(DatahubSourceProvider.OPTION_KEY_ACCESS_KEY, testUtils.accessKeySecret)
      .option(DatahubSourceProvider.OPTION_KEY_ENDPOINT, testUtils.endpoint)
      .option(DatahubSourceProvider.OPTION_KEY_PROJECT, testUtils.project)
      .option(DatahubSourceProvider.OPTION_KEY_TOPIC, "topic_emr_1")
      .option("checkpointLocation", "/tmp/spark/checkpoint")
      .trigger(Trigger.Continuous(200)).start()
    assert(query.isActive)
    query.stop()
  }

}
