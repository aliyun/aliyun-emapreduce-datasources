package org.apache.spark.sql.aliyun.tablestore

import com.alicloud.openservices.tablestore.model.tunnel.{TunnelStage, TunnelType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.Offset
import org.scalatest.FunSuite

import scala.util.Random

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
    println("Tunnel is ready")
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
        Map("catalog" -> catalog, "ots.tunnel" -> tunnelId, "maxOffsetsPerChannel" -> "10000")
      )

    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]

    for (_ <- 0 until 5) {
      val offset = source.getOffset
      System.out.println(offset.get)
      source.batches.values.map { rdd =>
        System.out.println(rdd.count())
        assert(rdd.count() == 10000)
      }
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
    println("Tunnel is ready")

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
        Map("catalog" -> catalog, "ots.tunnel" -> tunnelId, "maxOffsetsPerChannel" -> "50000")
      )

    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]

    val offset = source.getOffset
    System.out.println(offset.get)
    source.batches.values.map { rdd =>
      System.out.println(rdd.count())
      assert(rdd.count == 50000)
    }
  }

  test("GetOffset in random size stream") {
    setUp()
    val tunnelId = testUtils.createTunnel(TunnelType.Stream)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessStream)) {
      Thread.sleep(2000)
    }
    println("Tunnel is ready")

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
        Map("catalog" -> catalog, "ots.tunnel" -> tunnelId, "maxOffsetsPerChannel" -> "10000")
      )
    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]
    val rand = new Random(System.currentTimeMillis())
    for (i <- 0 to 20) {
      val count = rand.nextInt(1000)
      testUtils.insertData(count)
      println(s"Current batch write ${count} records.")
      val offset = source.getOffset
      source.commit(offset.get)
      System.out.println(offset.get)
      source.batches.values.map { rdd =>
        System.out.println(rdd.count())
        assert(rdd.count() == count)
      }
    }
  }

  test("GetOffset and GetBatch in random size stream") {
    setUp()
    val tunnelId = testUtils.createTunnel(TunnelType.Stream)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessStream)) {
      Thread.sleep(2000)
    }
    println("Tunnel is ready")

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
        Map("catalog" -> catalog, "ots.tunnel" -> tunnelId, "maxOffsetsPerChannel" -> "10000")
      )
    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]
    val rand = new Random(System.currentTimeMillis())
    var preOffset: Offset = TableStoreSourceOffset(source.initialPartitionOffsets)
    for (i <- 0 to 20) {
      val count = rand.nextInt(1000)
      testUtils.insertData(count)
      val offset = source.getOffset
      println(
        s"Current batch write ${count} records, preOffset: ${preOffset}, endOffset: ${offset}"
      )
      source.getBatch(Some(preOffset), offset.get)
      val rdd = source.currentBatchRDD
      System.out.println(rdd.count())
      assert(rdd.count() == count)
      preOffset = offset.get
      source.commit(offset.get)
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
    println("Tunnel is ready")

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
        Map("catalog" -> catalog, "ots.tunnel" -> tunnelId, "maxOffsetsPerChannel" -> "5000")
      )
    val source =
      testUtils.createTestSource(spark.sqlContext, options).asInstanceOf[TableStoreSource]
    // Base data
    var preOffset: Offset = TableStoreSourceOffset(source.initialPartitionOffsets)
    for (_ <- 0 until 2) {
      val offset = source.getOffset
      println(
        s"Current batch write 5000 records, preOffset: ${preOffset}, endOffset: ${offset}"
      )
      System.out.println(offset.get)
      source.getBatch(Some(preOffset), offset.get)
      val rdd = source.currentBatchRDD
      System.out.println(rdd.count())
      assert(rdd.count() == 5000)
      preOffset = offset.get
      source.commit(offset.get)
    }

    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessStream)) {
      Thread.sleep(2000)
    }
    println("Tunnel is ready")

    // Random stream data
    val rand = new Random(System.currentTimeMillis())
    for (i <- 0 to 20) {
      val count = rand.nextInt(1000)
      testUtils.insertData(count)
      val offset = source.getOffset
      println(
        s"Current batch write ${count} records, preOffset: ${preOffset}, endOffset: ${offset}"
      )
      source.getBatch(Some(preOffset), offset.get)
      val rdd = source.currentBatchRDD
      System.out.println(rdd.count())
      if (i >= 2) {
        assert(rdd.count() == count)
      }
      preOffset = offset.get
      source.commit(offset.get)
    }
  }
}
