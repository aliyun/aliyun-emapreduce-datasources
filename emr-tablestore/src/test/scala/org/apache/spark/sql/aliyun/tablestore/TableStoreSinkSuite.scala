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

import java.nio.charset.StandardCharsets

import com.alicloud.openservices.tablestore.model.{DefinedColumnSchema, DefinedColumnType,
  PrimaryKeyBuilder, PrimaryKeyValue}
import com.alicloud.openservices.tablestore.model.PrimaryKey

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class TableStoreSinkSuite extends SparkFunSuite {

  private val testUtils = new TableStoreTestUtil()
  private val logLevel = "INFO"

  def setUp(): Unit = {
    testUtils.deleteTable("spark_test1")
    testUtils.createTable("spark_test1", 5)
    Thread.sleep(2000)
  }

  private def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder
      .appName("TestGetOffset")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

  private def buildSchema(): StructType = {
    StructType(List(
      StructField("PkString", StringType, nullable = false),
      StructField("PkInt", LongType, nullable = false),
      StructField("col1", StringType, nullable = false),
      StructField("col2", LongType, nullable = false),
      StructField("col3", BinaryType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("col5", DoubleType, nullable = false),
      StructField("col6", BooleanType, nullable = false)
    ))
  }

  private def buildPrimaryKey(): PrimaryKey = {
    val primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder
    primaryKeyBuilder.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString("a"))
    primaryKeyBuilder.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(1L))
    primaryKeyBuilder.build
  }

  private def buildPrimaryKeys(): Array[PrimaryKey] = {
    val primaryKeyBuilder1 = PrimaryKeyBuilder.createPrimaryKeyBuilder
    primaryKeyBuilder1.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString("a"))
    primaryKeyBuilder1.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(1L))
    val primaryKeyBuilder2 = PrimaryKeyBuilder.createPrimaryKeyBuilder
    primaryKeyBuilder2.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString("a"))
    primaryKeyBuilder2.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(2L))
    Array(primaryKeyBuilder1.build, primaryKeyBuilder2.build)
  }

  test("sink v1") {
    setUp()

    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true)
    ))

    val df = spark.createDataFrame(rdd, buildSchema())
    df.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v1"
      ))
    ).save()

    assert(testUtils.getCount("spark_test1", buildPrimaryKeys()) == 2)
  }

  test("sink common v2") {
    setUp()

    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true)
    ))

    val df = spark.createDataFrame(rdd, buildSchema())
    df.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2"
      ))
    ).save()

    assert(testUtils.getCount("spark_test1", buildPrimaryKeys()) == 2)
  }

  test("writer.row.change.type = update") {
    setUp()

    val spark = createSparkSession()

    val rdd1 = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true)
    ))

    val rdd2 = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aaa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345679L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345679L, 1.1, true)
    ))

    val df1 = spark.createDataFrame(rdd1, buildSchema())

    val df2 = spark.createDataFrame(rdd2, buildSchema())

    // put test

    df1.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.row.change.type" -> "put"
      ))
    ).save()

    df2.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.row.change.type" -> "put"
      ))
    ).save()

    assert(testUtils.getRow("spark_test1", buildPrimaryKey(), 2).getColumns.length == 1 * 6)

    // update test

    df1.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.row.change.type" -> "update"
      ))
    ).save()

    assert(testUtils.getRow("spark_test1", buildPrimaryKey(), 2).getColumns.length == 2 * 6)

    assert(testUtils.getCount("spark_test1", buildPrimaryKeys()) == 2)
  }

  test("writer.batch.request.type = batch_write_row") {
    setUp()

    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true)
    ))

    val df = spark.createDataFrame(rdd, buildSchema())

    df.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.batch.request.type" -> "batch_write_row"
      ))
    ).save()

    assert(testUtils.getCount("spark_test1", buildPrimaryKeys()) == 2)
  }

  test("writer.batch.order.guaranteed = true") {
    setUp()

    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true)
    ))

    val df = spark.createDataFrame(rdd, buildSchema())

    df.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.batch.order.guaranteed" -> "true"
      ))
    ).save()

    assert(testUtils.getCount("spark_test1", buildPrimaryKeys()) == 2)
  }

  test("writer.batch.duplicate.allowed = true") {
    setUp()
    testUtils.deleteTable("spark_test2")
    testUtils.createTable("spark_test2", 1,
      Seq(new DefinedColumnSchema("col1", DefinedColumnType.STRING),
        new DefinedColumnSchema("timestamp", DefinedColumnType.INTEGER)).toArray
    )
    testUtils.createIndex("spark_test2", "spark_test2_index1")

    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 2L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true),
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345679L, 1.1, true)
    ), 1)

    val df = spark.createDataFrame(rdd, buildSchema())

    df.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test2",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.batch.order.guaranteed" -> "false",
        "writer.batch.duplicate.allowed" -> "true"
      ))
    ).save()

    assert(testUtils.getCount("spark_test2", buildPrimaryKeys()) == 2)
  }

  test("spark.ignore.on-failure.enabled = true, for dirty data") {
    setUp()
    val spark = createSparkSession()

    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", 1L, "aa", 1L, "1".getBytes(StandardCharsets.UTF_8), 12345678L, 1.1, true)
    ))

    val df = spark.createDataFrame(rdd, buildSchema())

    df.write.format("tablestore").options(testUtils.getUnionOptions(
      Map(
        "table.name" -> "spark_test1",
        "catalog" -> TableStoreTestUtil.catalog,
        "maxOffsetsPerChannel" -> "10000",
        "version" -> "v2",
        "writer.max.column.count" -> "3",
        "writer.batch.duplicate.allowed" -> "false",
        "spark.ignore.on-failure.enabled" -> "true"
      ))
    ).save()

    assert(testUtils.getRow("spark_test1", buildPrimaryKey(), 2) == null)
  }

}
