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

package org.apache.spark.sql.aliyun.tablestore

import scala.util.Random

import com.alicloud.openservices.tablestore.model.tunnel.{TunnelStage, TunnelType}
import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.Offset

class TableStoreMicroBatchSourceSuite extends FunSuite {

  private val testUtils = new TableStoreTestUtil()
  private val logLevel = "INFO"

  def setUp(): Unit = {
    testUtils.deleteTunnel()
    testUtils.deleteTable()
    testUtils.createTable()
    Thread.sleep(2000)
  }

  test("GetOffset in several batches") {
    // Create Table and insert Data.
    setUp()
    testUtils.insertData(50000)
    val tunnelId = testUtils.createTunnel(TunnelType.BaseData)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessBaseData)) {
      Thread.sleep(2000)
    }
    val spark = SparkSession.builder
      .appName("TestGetOffset")
      .master("local[5]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog, "tunnel.id" -> tunnelId, "maxOffsetsPerChannel" -> "10000")
      )

    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]

    var preOffset: Offset = TableStoreSourceOffset(source.initialOffsetUUID)
    for (_ <- 0 until 5) {
      val endOffset = source.getOffset
      source.getBatch(Some(preOffset), endOffset.get)
      val rdd = source.currentBatchRDD
      assert(rdd.count() == 10000)
      preOffset = endOffset.get
    }
  }

  test("GetOffset in one batch") {
    // Create Table and insert Data.
    setUp()
    testUtils.insertData(50000)
    val tunnelId = testUtils.createTunnel(TunnelType.BaseData)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessBaseData)) {
      Thread.sleep(2000)
    }

    val spark = SparkSession.builder
      .appName("TestGetOffset")
      .master("local[5]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog, "tunnel.id" -> tunnelId, "maxOffsetsPerChannel" -> "50000")
      )

    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]

    val preOffset = TableStoreSourceOffset(source.initialOffsetUUID)
    val endOffset = source.getOffset
    source.getBatch(Some(preOffset), endOffset.get)
    val rdd = source.currentBatchRDD
    assert(rdd.count() == 50000)
  }

  test("GetOffset in random size stream") {
    setUp()
    val tunnelId = testUtils.createTunnel(TunnelType.Stream)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessStream)) {
      Thread.sleep(2000)
    }

    val spark = SparkSession.builder
      .appName("TestGetOffset")
      .master("local[5]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog, "tunnel.id" -> tunnelId, "maxOffsetsPerChannel" -> "10000")
      )
    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]
    val rand = new Random(System.currentTimeMillis())
    var preOffset: Offset = TableStoreSourceOffset(source.initialOffsetUUID)
    for (i <- 0 to 20) {
      val count = rand.nextInt(1000)
      testUtils.insertData(count)
      val endOffset = source.getOffset
      source.getBatch(Some(preOffset), endOffset.get)
      val rdd = source.currentBatchRDD
      assert(rdd.count() == count)
      preOffset = endOffset.get
    }
  }

  test("GetOffset and GetBatch in random size stream") {
    setUp()
    val tunnelId = testUtils.createTunnel(TunnelType.Stream)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessStream)) {
      Thread.sleep(2000)
    }

    val spark = SparkSession.builder
      .appName("TestGetOffset")
      .master("local[5]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog, "tunnel.id" -> tunnelId, "maxOffsetsPerChannel" -> "10000")
      )
    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]
    val rand = new Random(System.currentTimeMillis())
    var preOffset: Offset = TableStoreSourceOffset(source.initialOffsetUUID)
    for (i <- 0 to 20) {
      val count = rand.nextInt(1000)
      testUtils.insertData(count)
      val endOffset = source.getOffset
      source.commit(preOffset)
      source.getBatch(Some(preOffset), endOffset.get)
      val rdd = source.currentBatchRDD
      assert(rdd.count() == count)
      preOffset = endOffset.get
    }
  }

  test("GetOffset and GetBatch in channel partition change") {
    setUp()
    // Cause TableStore partition interface is not open, use BaseAndStream type to simulate.
    testUtils.insertData(10000)
    val tunnelId = testUtils.createTunnel(TunnelType.BaseAndStream)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessBaseData)) {
      Thread.sleep(2000)
    }

    val spark = SparkSession.builder
      .appName("TestGetOffset")
      .master("local[5]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog, "tunnel.id" -> tunnelId, "maxOffsetsPerChannel" -> "5000")
      )
    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]
    // Base data
    var preOffset: Offset = TableStoreSourceOffset(source.initialOffsetUUID)
    for (_ <- 0 until 2) {
      val offset = source.getOffset
      source.commit(preOffset)
      source.getBatch(Some(preOffset), offset.get)
      val rdd = source.currentBatchRDD
      assert(rdd.count() == 5000)
      preOffset = offset.get
    }
    source.commit(preOffset)

    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessStream)) {
      Thread.sleep(2000)
    }

    // Random stream data
    val rand = new Random(System.currentTimeMillis())
    for (i <- 0 to 20) {
      val count = rand.nextInt(1000)
      testUtils.insertData(count)
      val offset = source.getOffset
      source.commit(preOffset)
      source.getBatch(Some(preOffset), offset.get)
      val rdd = source.currentBatchRDD
      assert(rdd.count() == count)
      preOffset = offset.get
    }
  }
}
