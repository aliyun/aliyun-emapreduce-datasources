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

import java.nio.file.Paths
import java.util.concurrent.ConcurrentLinkedQueue

import scala.io.Source

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

abstract class LoghubSourceTest extends StreamTest with SharedSQLContext {
  var testUtils: LoghubTestUtils = _
  val defaultSchema = StructType(Array(StructField("msg", StringType)))

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

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    q match {
      case c: ContinuousExecution => c.awaitEpoch(0)
      case m: MicroBatchExecution => m.processAllAvailable()
    }
    true
  }

  /**
   * Add data to LogStore.
   *
   * `action` can be used to run actions for each logStore before inserting data.
   */
  case class AddLogStoreData(logStore: String, shardId: Option[Int], data: Int*)
    (implicit ensureDataInMultiplePartition: Boolean = false,
      concurrent: Boolean = false,
      message: String = "",
      action: (String, Option[Int]) => Unit = (_, _) => {}) extends AddData {

    override def addData(query: Option[StreamExecution]): (BaseStreamingSource, Offset) = {
      query match {
        // Make sure no Spark job is running when deleting a topic
        case Some(m: MicroBatchExecution) => m.processAllAvailable()
        case _ =>
      }

      val existingLogStores = testUtils.getAllLogStoreAndShardSize()
      val newLogStores = Set(logStore).diff(existingLogStores.keySet)
      for (newStore <- newLogStores) {
        action(newStore, None)
      }
      for (existingTopicPartitions <- existingLogStores) {
        action(existingTopicPartitions._1, Some(existingTopicPartitions._2))
      }

      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active loghub source")

      val sources = {
        query.get.logicalPlan.collect {
          case StreamingExecutionRelation(source: LoghubSource, _) => source
        } ++ (query.get.lastExecution match {
          case null => Seq()
          case e => e.logical.collect {
            case StreamingDataSourceV2Relation(_, _, _, reader: LoghubContinuousReader) => reader
          }
        })
      }.distinct

      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Loghub source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Loghub source in the StreamExecution logical plan as there" +
            "are multiple Loghub sources:\n\t" + sources.mkString("\n\t"))
      }
      val loghubSource = sources.head
      testUtils.sendMessages(data.map { _.toString }.toList, logStore, shardId)
      Thread.sleep(5000)
      val offset = LoghubSourceOffset(testUtils.getLatestOffsets(logStore))
      logInfo(s"Added data, expected offset $offset")
      (loghubSource, offset)
    }

    override def toString: String =
      s"AddLoghubData(logStore = $logStore, shardId = $shardId, data = $data, message = $message)"
  }
}

abstract class LoghubSourceSuiteBase extends LoghubSourceTest {
  import testImplicits._

  override val streamingTimeout = 30.seconds

  test("cannot stop Loghub stream") {
    val logStore = testUtils.getOneLogStore()
    testUtils.sendMessages((101 to 105).map(_.toString).toList, logStore)

    val reader = spark
      .readStream
      .format("loghub")
      .schema(defaultSchema)
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)

    val logstore = reader.load()
      .select("msg")
      .as[String]
    val mapped = logstore.map(msg => msg.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  for (splitShard <- Array(true, false)) {
    test(s"read from latest offsets (splitShard: $splitShard)") {
      testFromLatestOffsets(splitShard = splitShard)
    }

    test(s"read from earliest offsets (splitShard: $splitShard)") {
      testFromEarliestOffsets(splitShard = splitShard)
    }
  }

  private def testFromLatestOffsets(
      splitShard: Boolean,
      options: (String, String)*): Unit = {
    val logStore = testUtils.getOneLogStore()
    testUtils.sendMessages(List("-1"), logStore)
    require(testUtils.getLatestOffsets(logStore).size === 2)
    Thread.sleep(5000)

    val reader = spark
      .readStream
      .format("loghub")
      .schema(defaultSchema)
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .option("startingOffsets", "latest")

    options.foreach { case (k, v) => reader.option(k, v) }
    val loghub = reader.load().select("msg").as[String]
    val mapped = loghub.map(msg => msg.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddLogStoreData(logStore, shardId = None, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddLogStoreData(logStore, shardId = None, 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddLogStoreData(logStore, shardId = None, 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Split shards") { _: StreamExecution =>
        if (splitShard) testUtils.splitLoghubShards(logStore)
        true
      },
      AddLogStoreData(logStore, shardId = Some(2), 9, 10, 11, 12),
      AddLogStoreData(logStore, shardId = Some(3), 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestOffsets(
      splitShard: Boolean,
      options: (String, String)*): Unit = {
    val logStore = testUtils.getOneLogStore()
    testUtils.sendMessages((1 to 3).map { _.toString }.toList, logStore)
    require(testUtils.getLatestOffsets(logStore).size === 2)
    Thread.sleep(5000)

    val reader = spark.readStream
    reader
      .format(classOf[LoghubSourceProvider].getCanonicalName.stripSuffix("$"))
      .schema(defaultSchema)
      .option("startingOffsets", "earliest")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
    options.foreach { case (k, v) => reader.option(k, v) }
    val loghub = reader.load()
      .select("msg")
      .as[String]
    val mapped = loghub.map(msg => msg.toInt + 1)

    testStream(mapped)(
      AddLogStoreData(logStore, shardId = None, 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddLogStoreData(logStore, shardId = None, 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Split shards") { _: StreamExecution =>
        if (splitShard) testUtils.splitLoghubShards(logStore)
        true
      },
      AddLogStoreData(logStore, shardId = Some(2), 9, 10, 11, 12),
      AddLogStoreData(logStore, shardId = Some(3), 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }
}

abstract class LoghubMicroBatchSourceSuiteBase extends LoghubSourceSuiteBase {
  import testImplicits._

  test("(de)serialization of initial offsets") {
    val logStore = testUtils.getOneLogStore()

    val reader = spark
      .readStream
      .format("loghub")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)

    testStream(reader.load)(
      makeSureGetOffsetCalled,
      StopStream,
      StartStream(),
      StopStream)
  }

  test("SPARK-26718 Rate limit set to Long.Max should not overflow integer " +
    "during end offset calculation") {
    val logStore = testUtils.getOneLogStore()
    // fill in 5 messages to trigger potential integer overflow
    testUtils.sendMessages((0 to 5).map { _.toString }.toList, logStore, Some(0))
    Thread.sleep(5000)
    val currentTime = (System.currentTimeMillis() / 1000).toInt

    val startPartitionOffsets = Map(
      LoghubShard(testUtils.logProject, logStore, 0) -> (currentTime, ""),
      LoghubShard(testUtils.logProject, logStore, 1) -> (currentTime, "")
    )
    val startingOffsets = LoghubSourceOffset.partitionOffsets(startPartitionOffsets)

    val loghub = spark
      .readStream
      .format("loghub")
      .schema(defaultSchema)
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      // use latest to force begin to be 5
      .option("startingOffsets", startingOffsets)
      // use Long.Max to try to trigger overflow
      .option("maxOffsetsPerTrigger", Long.MaxValue)
      .load()
      .select("msg")
      .as[String]
    val mapped: org.apache.spark.sql.Dataset[_] = loghub.map(d => d.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddLogStoreData(logStore, None, 30, 31, 32, 33, 34),
      CheckAnswer(30, 31, 32, 33, 34),
      StopStream
    )
  }

  test("input row metrics") {
    val logStore = testUtils.getOneLogStore()
    testUtils.sendMessages(List("-1"), logStore)
    Thread.sleep(5000)
    require(testUtils.getLatestOffsets(logStore).size === 2)

    val loghub = spark
      .readStream
      .format("loghub")
      .schema(defaultSchema)
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .load()
      .select("msg")
      .as[String]

    val mapped = loghub.map(d => d.toInt + 1)
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddLogStoreData(logStore, None, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  test("ensure that initial offset are written with an extra byte in the beginning (SPARK-19517)") {
    withTempDir { metadataPath =>
      val logStore = testUtils.getOneLogStore()

      val initialOffsetFile = Paths.get(s"${metadataPath.getAbsolutePath}/sources/0/0").toFile

      val df = spark
        .readStream
        .format("loghub")
        .schema(defaultSchema)
        .option("sls.project", testUtils.logProject)
        .option("sls.store", logStore)
        .option("access.key.id", testUtils.accessKeyId)
        .option("access.key.secret", testUtils.accessKeySecret)
        .option("endpoint", testUtils.endpoint)
        .option("startingOffsets", s"earliest")
        .load()

      // Test the written initial offset file has 0 byte in the beginning, so that
      // Spark 2.1.0 can read the offsets (see SPARK-19517)
      testStream(df)(
        StartStream(checkpointLocation = metadataPath.getAbsolutePath),
        makeSureGetOffsetCalled)

      val binarySource = Source.fromFile(initialOffsetFile)
      try {
        assert(binarySource.next().toInt == 0)  // first byte is binary 0
      } finally {
        binarySource.close()
      }
    }
  }

  test("LoghubSource with watermark") {
    val now = System.currentTimeMillis()
    val logStore = testUtils.getOneLogStore()
    testUtils.sendMessages(List("1"), logStore)
    Thread.sleep(5000)

    val loghub = spark
      .readStream
      .format("loghub")
      .schema(StructType(Array(StructField("msg", StringType), StructField("__time__", TimestampType))))
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .option("startingOffsets", s"earliest")
      .load()
      .selectExpr("msg", "__time__ AS timestamp")

    val windowedAggregation = loghub
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start") as 'window, $"count")

    val query = windowedAggregation
      .writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("LoghubWatermark")
      .start()
    query.processAllAvailable()
    val rows = spark.table("loghubWatermark").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    // We cannot check the exact window start time as it depands on the time that messages were
    // inserted by the producer. So here we just use a low bound to make sure the internal
    // conversion works.
    assert(
      row.getAs[java.sql.Timestamp]("window").getTime >= now - 5 * 1000,
      s"Unexpected results: $row")
    assert(row.getAs[Int]("count") === 1, s"Unexpected results: $row")
    query.stop()
  }

  test("ensure stream-stream self-join generates only one offset in log and correct metrics") {
    val logStore = testUtils.getOneLogStore()
    require(testUtils.getLatestOffsets(logStore).size === 2)

    val loghub = spark
      .readStream
      .format("loghub")
      .schema(defaultSchema)
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .load()

    val values = loghub
      .selectExpr("CAST(CAST(msg AS STRING) AS INT) AS value",
        "CAST(CAST(msg AS STRING) AS INT) % 5 AS key")

    val join = values.join(values, "key")

    testStream(join)(
      makeSureGetOffsetCalled,
      AddLogStoreData(logStore, None, 1, 2),
      CheckAnswer((1, 1, 1), (2, 2, 2)),
      AddLogStoreData(logStore, None, 6, 3),
      CheckAnswer((1, 1, 1), (2, 2, 2), (3, 3, 3), (1, 6, 1), (1, 1, 6), (1, 6, 6)),
      AssertOnQuery { q =>
        assert(q.availableOffsets.iterator.size == 1)
        assert(q.recentProgress.map(_.numInputRows).sum == 4)
        true
      }
    )
  }
}

class LoghubMicroBatchV1SourceSuite extends LoghubMicroBatchSourceSuiteBase {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      "spark.sql.streaming.disabledV2MicroBatchReaders",
      classOf[LoghubSourceProvider].getCanonicalName)
  }

  test("V1 Source is used when disabled through SQLConf") {
    val logStore = testUtils.getOneLogStore()

    val loghub = spark
      .readStream
      .format("loghub")
      .option("sls.project", testUtils.logProject)
      .option("sls.store", logStore)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("endpoint", testUtils.endpoint)
      .load()

    testStream(loghub)(
      makeSureGetOffsetCalled,
      AssertOnQuery { query =>
        query.logicalPlan.collect {
          case StreamingExecutionRelation(_: LoghubSource, _) => true
        }.nonEmpty
      }
    )
  }
}

object LoghubSourceSuite {
  @volatile var globalTestUtils: LoghubTestUtils = _
  val collectedData = new ConcurrentLinkedQueue[Any]()
}
