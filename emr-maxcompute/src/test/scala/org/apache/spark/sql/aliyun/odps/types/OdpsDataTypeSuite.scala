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

package org.apache.spark.sql.aliyun.odps.types

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

class OdpsDataTypeSuite extends SparkFunSuite {
  val accessKeyId: String = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
  val accessKeySecret: String = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")

  val envType: Int = {
    val envType = Option(System.getenv("TEST_ENV_TYPE")).getOrElse("public").toLowerCase
    if (envType != "private" && envType != "public") {
      throw new Exception(
        s"Unsupported test environment type: $envType, only support private or public")
    }
    if (envType.equals("public")) 0 else 1
  }
  // Update this with your own testing odps project.
  val project: String = Option(System.getenv("ODPS_PROJECT_NAME")).getOrElse("")
  val numPartitions = 2

  val urls = Seq(
    // public environment
    Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"),
    // Aliyun internal environment
    Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com")
  )

  val conf = new SparkConf().setAppName("Test Odps Read").setMaster("local[*]")
  val ss = SparkSession.builder().appName("Test Odps Read").master("local[*]").getOrCreate()

  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  override def beforeAll(): Unit = {
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0))
    odpsUtils.runSQL(project,
      // scalastyle:off
      """
        |drop table if exists odps_basic_types;
        |CREATE TABLE `odps_basic_types` (
        |    `a` boolean,
        |    `b` smallint,
        |    `c` int,
        |    `d` bigint,
        |    `e` float,
        |    `f` double,
        |    `g` decimal,
        |    `h` datetime,
        |    `i` timestamp,
        |    `j` string,
        |    `k` tinyint,
        |    `l` binary,
        |    `m` array<double>,
        |    `n` map<double, timestamp>,
        |    `o` struct<s1: double, s2: timestamp>
        |) ;
      """.stripMargin,
      // scalastyle:on
      Map(
        "odps.sql.type.system.odps2" -> "true",
        "odps.sql.submit.mode" -> "script"
      ))
  }

  override def afterAll(): Unit = {
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0))
    odpsUtils.runSQL(project, "TRUNCATE TABLE odps_basic_types;")
  }

  test("[OdpsDataTypeSuite] support basic types") {
    val table = "odps_basic_types"
    val struct = StructType(
        StructField("a", BooleanType, true) ::
        StructField("b", ShortType, true) ::
        StructField("c", IntegerType, true) ::
        StructField("d", LongType, true) ::
        StructField("e", FloatType, true) ::
        StructField("f", DoubleType, true) ::
        StructField("g", DecimalType.SYSTEM_DEFAULT, true) ::
        StructField("h", DateType, true) ::
        StructField("i", TimestampType, true) ::
        StructField("j", StringType, true) ::
        StructField("k", ByteType, true) ::
        StructField("l", BinaryType, true) ::
        StructField("m", ArrayType(DoubleType), true) ::
        StructField("n", MapType(DoubleType, TimestampType)) ::
        StructField("o", StructType(Array(StructField("s1", DoubleType), StructField("s2", TimestampType)))) ::
        Nil)

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm", Locale.US)
    val dataSeq = Array(
      (false, 1.toShort, 2, 3L, 4.0f, 5.0d, new java.math.BigDecimal("3.520122999999999891"),
        new Date(dateFormat.parse("26/10/2015 18:00").getTime),
        new Timestamp(1510429612345L), "test-string", "6".toByte, testBytes,
        Array(1.1, 2.2),
        Map(1.2 -> new Timestamp(1510429612346L)),
        Row(1.2, new Timestamp(1510429612347L))
      ))
    val rowRDD = ss.sparkContext.makeRDD(dataSeq).map(attributes => Row(
      attributes._1,
      attributes._2,
      attributes._3,
      attributes._4,
      attributes._5,
      attributes._6,
      attributes._7,
      attributes._8,
      attributes._9,
      attributes._10,
      attributes._11,
      attributes._12,
      attributes._13,
      attributes._14,
      attributes._15
    ))

    val df = ss.createDataFrame(rowRDD, struct)
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()

    assert(readDF.schema.fieldNames === struct.fieldNames)
    val collectList = readDF.collect()
    dataSeq.zip(collectList).foreach(e => {
      Row.fromTuple(e._1).toSeq.zip(e._2.toSeq).foreach(t => {
        if (t._1.isInstanceOf[Array[Byte]] && t._2.isInstanceOf[Array[Byte]]) {
          t._1.asInstanceOf[Array[Byte]].deep == t._2.asInstanceOf[Array[Byte]].deep
        } else {
          t._1.toString == t._2.toString
        }
      })
    })
  }
}
