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

package org.apache.spark.sql.aliyun.logservice

import java.util.{Locale, UUID}

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class LoghubSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._
  private var testUtils: LoghubTestUtils = _
  private val defaultSchema = StructType(Array(StructField("msg", StringType)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new LoghubTestUtils()
    testUtils.cleanAllResources()
    testUtils.init()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.cleanAllResources()
    }
  }

  private def createDF(
      store: String,
      schema: Option[StructType],
      columnName: String = "__value__",
      withOptions: Map[String, String] = Map.empty[String, String]) = {
    val df = spark
      .read
      .format("loghub")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", store)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
    if (schema.isDefined) {
      df.schema(schema.get)
    }
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().select(columnName)
  }

  test("batch - write to log store") {
    val logStore = testUtils.getOneLogStore()
    val df = Seq("1", "2", "3", "4", "5").toDF("msg")
    df.write
      .format("loghub")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .save()
    // Wait 5 seconds to make sure data are available to read.
    Thread.sleep(5000)
    checkAnswer(
      createDF(logStore, Some(defaultSchema), "msg",
        withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest")),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("batch - unsupported save modes") {
    val logStore = testUtils.getOneLogStore()
    val df = Seq("1", "2", "3", "4", "5").toDF("msg")

    // Test bad save mode Ignore
    var ex = intercept[AnalysisException] {
      df.write
        .format("loghub")
        .option("sls.project", testUtils.logProject)
        .option("sls.store", logStore)
        .option("access.key.id", testUtils.accessKeyId)
        .option("access.key.secret", testUtils.accessKeySecret)
        .option("endpoint", testUtils.endpoint)
        .mode(SaveMode.Ignore)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      s"save mode ignore not allowed for loghub"))

    // Test bad save mode Overwrite
    ex = intercept[AnalysisException] {
      df.write
        .format("loghub")
        .option("sls.project", testUtils.logProject)
        .option("sls.store", logStore)
        .option("access.key.id", testUtils.accessKeyId)
        .option("access.key.secret", testUtils.accessKeySecret)
        .option("endpoint", testUtils.endpoint)
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      s"save mode overwrite not allowed for loghub"))
  }

  test("batch - enforce analyzed plans") {
    val inputEvents = spark.range(1, 1000).toDF("msg")

    val logStore = testUtils.getOneLogStore()
    // used to throw UnresolvedException
    inputEvents.write
      .format("loghub")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .save()
  }

  test("streaming - write to loghub") {
    val input = MemoryStream[String]
    val logStore = testUtils.getOneLogStore()

    val writer = createLoghubWriter(
      input.toDF(),
      logStore,
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = "value as msg")

    val reader = createLoghubReader(logStore, Some(defaultSchema))
      .selectExpr("CAST(msg as INT) msg")
      .as[Int]

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      Thread.sleep(5000)
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      Thread.sleep(5000)
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write aggregation to loghub") {
    val input = MemoryStream[String]
    val logStore = testUtils.getOneLogStore()

    val writer = createLoghubWriter(
      input.toDF().groupBy("value").count(),
      logStore,
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr = "value as key", "count")

    val reader = createLoghubReader(logStore, Some(
        StructType(Array(StructField("key", StringType), StructField("count", IntegerType)))))
      .selectExpr("CAST(key as INT) key", "count")
      .as[(Int, Int)]

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(30.seconds) {
        writer.processAllAvailable()
      }
      Thread.sleep(5000)
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3))
      input.addData("1", "2", "3")
      failAfter(30.seconds) {
        writer.processAllAvailable()
      }
      Thread.sleep(5000)
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4))
    } finally {
      writer.stop()
    }
  }

  test("streaming - write to non-existing logstore") {
    val input = MemoryStream[String]
    val logStore = s"non-existing-logstore-${UUID.randomUUID()}"

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createLoghubWriter(input.toDF(), logStore)(withSelectExpr = "value as msg")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("job aborted"))
  }

  private def createLoghubReader(logStore: String, schema: Option[StructType]): DataFrame = {
    val dfr = spark.read
      .format("loghub")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
    schema match {
      case Some(s) => dfr.schema(s)
      case None => // ok
    }
    dfr.load()
  }

  private def createLoghubWriter(
      input: DataFrame,
      logStore: String,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.length > 0) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("loghub")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("sls.project", testUtils.logProject)
        .option("sls.store", logStore)
        .option("access.key.id", testUtils.accessKeyId)
        .option("access.key.secret", testUtils.accessKeySecret)
        .option("endpoint", testUtils.endpoint)
        .queryName("loghubStream")
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }
}
