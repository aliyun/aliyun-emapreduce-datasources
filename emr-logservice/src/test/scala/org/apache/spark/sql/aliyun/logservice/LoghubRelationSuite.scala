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

import java.util.Locale

import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class LoghubRelationSuite extends QueryTest with SharedSQLContext {
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

  test("explicit earliest to latest offsets") {
    val logStore = testUtils.getOneLogStore()

    testUtils.sendMessages((0 to 9).map(_.toString).toList, logStore)
    testUtils.sendMessages((10 to 19).map(_.toString).toList, logStore)
    testUtils.sendMessages(List("20"), logStore)
    Thread.sleep(5000)

    val df = createDF(logStore, Some(defaultSchema), "msg",
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)
  }

  test("default starting and ending offsets") {
    val logStore = testUtils.getOneLogStore()

    testUtils.sendMessages((0 to 9).map(_.toString).toList, logStore)
    testUtils.sendMessages((10 to 19).map(_.toString).toList, logStore)
    testUtils.sendMessages(List("20"), logStore)
    Thread.sleep(5000)

    val df = createDF(logStore, Some(defaultSchema), "msg")
    checkAnswer(df, (0 to 20).map(_.toString).toDF)
  }

  test("explicit offsets") {
    val logStore = testUtils.getOneLogStore()

    testUtils.sendMessages((0 to 9).map(_.toString).toList, logStore, Some(0))
    testUtils.sendMessages(List("10"), logStore, Some(1))
    Thread.sleep(5000)

    val startPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (-2, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (-2, "")
    )
    val startingOffsets = LoghubSourceOffset.partitionOffsets(startPartitionOffsets)

    val endPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (-1, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (-1, "")
    )
    val endingOffsets = LoghubSourceOffset.partitionOffsets(endPartitionOffsets)

    val df = createDF(logStore, Some(defaultSchema), "msg",
      withOptions = Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets))

    checkAnswer(df, (0 to 10).map(_.toString).toDF)

    testUtils.sendMessages((11 to 20).map(_.toString).toList, logStore, Some(0))
    Thread.sleep(5000)
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    testUtils.sendMessages((21 to 30).map(_.toString).toList, logStore, Some(1))
    Thread.sleep(5000)
    checkAnswer(df, (0 to 30).map(_.toString).toDF)
  }

  test("invalid start and end offset in batch query") {
    val logStore = testUtils.getOneLogStore()
    var startPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (-2, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (-1, "")
    )
    var startingOffsets = LoghubSourceOffset.partitionOffsets(startPartitionOffsets)

    var endPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (-1, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (-1, "")
    )
    var endingOffsets = LoghubSourceOffset.partitionOffsets(endPartitionOffsets)
    intercept[IllegalArgumentException] {
      createDF(logStore, None,
        withOptions = Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets))
    }

    startPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (-2, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (-2, "")
    )
    startingOffsets = LoghubSourceOffset.partitionOffsets(startPartitionOffsets)

    endPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (-2, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (-1, "")
    )
    endingOffsets = LoghubSourceOffset.partitionOffsets(endPartitionOffsets)

    intercept[IllegalArgumentException] {
      createDF(logStore, None,
        withOptions = Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets))
    }
  }

  test("reuse same dataframe in query") {
    val logStore = testUtils.getOneLogStore()
    testUtils.sendMessages((0 to 10).map(_.toString).toList, logStore)
    Thread.sleep(5000)

    // Specify explicit earliest and latest offset values
    val df = createDF(logStore, Some(defaultSchema), "msg",
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }

  test("bad batch query options") {
    def testMissingOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[MissingArgumentException] {
        val reader = spark
          .read
          .format("loghub")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testMissingOptions()("Missing logService project (='sls.project').")

    // Multiple strategies specified
    testMissingOptions("sls.project" -> "p")("Missing logService store (='sls.store').")

    testMissingOptions("sls.project" -> "p", "sls.store" -> "s")(
      "Missing access key id (='access.key.id').")

    testMissingOptions("sls.project" -> "p", "sls.store" -> "s", "access.key.id" -> "id")(
      "Missing access key secret (='access.key.secret').")
    testMissingOptions("sls.project" -> "p", "sls.store" -> "s",
      "access.key.id" -> "id", "access.key.secret" -> "key")(
      "Missing log store endpoint (='endpoint').")
  }
}
