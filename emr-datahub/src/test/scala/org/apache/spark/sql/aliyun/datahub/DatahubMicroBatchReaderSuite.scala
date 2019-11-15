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

import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous.ContinuousExecution
import org.apache.spark.sql.streaming.{ProcessingTime, StreamTest}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class DatahubMicroBatchReaderSuite extends QueryTest with SharedSQLContext with StreamTest {
  import testImplicits._

  private var testUtils: DatahubTestUtils = _

  private val defaultSchema = StructType(Array(StructField("msg", StringType)))

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new DatahubTestUtils()
    testUtils.init()
    testUtils.cleanAllResource()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.cleanAllResource()
      testUtils = null
    }
    super.afterAll()
  }

  private def createDF(
      topic: String,
      withOptions: Map[String, String] = Map.empty[String, String]): DataFrame = {
    val df = spark
      .read
      .format("datahub")
      .option("endpoint", testUtils.endpoint)
      .option("project", testUtils.project)
      .option("topic", topic)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("decimal.precision", "5")
      .option("decimal.scale", "5")
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().selectExpr("CAST(value AS STRING)")
  }

  def makeSureGetOffsetCalled = AssertOnQuery { q =>
    q match {
      case c: ContinuousExecution => c.awaitEpoch(0)
      case m: MicroBatchExecution => m.processAllAvailable()
    }
    true
  }

  /**
   * Add data to Datahub.
   *
   * `action` can be used to run actions for each datahub before inserting data.
   */
  case class AddDatahubData(topic: String, shardId: Option[Int], data: Int*)
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

      val existingTopics = testUtils.getAllTopicAndShardSize()
      val newTopics = Set(topic).diff(existingTopics.keySet.map(_.topic))
      for (newTopic <- newTopics) {
        action(newTopic, None)
      }
      for (existingTopicShards <- existingTopics) {
        action(existingTopicShards._1.topic, Some(existingTopicShards._2))
      }

      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active datahub source")

      val sources = {
        query.get.logicalPlan.collect {
          case StreamingExecutionRelation(source: DatahubSource, _) => source
        } ++ (query.get.lastExecution match {
          case null => Seq()
          case e => e.logical.collect {
            case StreamingDataSourceV2Relation(_, _, _, reader: DatahubMicroBatchReader) => reader
            case StreamingDataSourceV2Relation(_, _, _, reader: DatahubContinuousReader) => reader
          }
        })
      }.distinct

      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Datahub source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Datahub source in the StreamExecution logical plan as there" +
            "are multiple Datahub sources:\n\t" + sources.mkString("\n\t"))
      }
      val datahubSource = sources.head
      testUtils.sendMessage(topic, shardId, data.map { _.toString }:_*)
      Thread.sleep(5000)
      val offset = DatahubSourceOffset(testUtils.getLatestOffsets(topic))
      logInfo(s"Added data, expected offset $offset")
      (datahubSource, offset)
    }

    override def toString: String =
      s"AddDatahubData(topic = $topic, data = $data, message = $message)"
  }

  test("cannot stop Datahub stream") {
    val topic = testUtils.createTopic(defaultSchema)
    testUtils.sendMessage(topic, None, (101 to 105).map(_.toString):_*)

    val reader = spark
      .readStream
      .format("datahub")
      .option("endpoint", testUtils.endpoint)
      .option("project", testUtils.project)
      .option("topic", topic)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("decimal.precision", "5")
      .option("decimal.scale", "5")

    val datahub = reader.load()
      .select("msg")
      .as[String]
    val mapped = datahub.map(msg => msg.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  test("(de)serialization of initial offsets") {
    val topic = testUtils.createTopic(defaultSchema)

    val reader = spark
      .readStream
      .format("datahub")
      .option("endpoint", testUtils.endpoint)
      .option("project", testUtils.project)
      .option("topic", topic)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("decimal.precision", "5")
      .option("decimal.scale", "5")

    testStream(reader.load)(
      makeSureGetOffsetCalled,
      StopStream,
      StartStream(),
      StopStream)
  }

  test("input row metrics") {
    val topic = testUtils.createTopic(defaultSchema)
    testUtils.sendMessage(topic, None, Array("-1"):_*)
    Thread.sleep(5000)
    require(testUtils.getLatestOffsets(topic).size === 2)

    val datahub = spark
      .readStream
      .format("datahub")
      .option("endpoint", testUtils.endpoint)
      .option("project", testUtils.project)
      .option("topic", topic)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("decimal.precision", "5")
      .option("decimal.scale", "5")
      .load()
      .select("msg")
      .as[String]

    val mapped = datahub.map(d => d.toInt + 1)
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddDatahubData(topic, shardId = None, 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  test("SPARK-26718 Rate limit set to Long.Max should not overflow integer " +
    "during end offset calculation") {
    val topic = testUtils.createTopic(defaultSchema)
    testUtils.sendMessage(topic, Some(0), (0 to 5).map { _.toString }:_*)
    val startPartitionOffsets = Map(
      DatahubShard(testUtils.project, topic , "0") -> 6L,
      DatahubShard(testUtils.project, topic, "1") -> 0L
    )
    val startingOffsets = DatahubSourceOffset.partitionOffsets(startPartitionOffsets)

    val loghub = spark
      .readStream
      .format("datahub")
      .option("endpoint", testUtils.endpoint)
      .option("project", testUtils.project)
      .option("topic", topic)
      .option("access.key.id", testUtils.accessKeyId)
      .option("access.key.secret", testUtils.accessKeySecret)
      .option("decimal.precision", "5")
      .option("decimal.scale", "5")
      // use latest to force begin to be 6
      .option("startingoffsets", startingOffsets)
      // use Long.Max to try to trigger overflow
      .option("maxOffsetsPerTrigger", Long.MaxValue)
      .load()
      .select("msg")
      .as[String]
    val mapped: org.apache.spark.sql.Dataset[_] = loghub.map(d => d.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddDatahubData(topic, shardId = None, 30, 31, 32, 33, 34),
      CheckAnswer(30, 31, 32, 33, 34),
      StopStream
    )
  }
}
