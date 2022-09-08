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

package org.apache.spark.sql.aliyun.odps

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.{Binary, Record}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.aliyun.odps.OdpsOps
import org.apache.spark.aliyun.odps.utils.OdpsUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class OdpsOpsSuite extends SparkFunSuite {
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
  val ss = SparkSession.builder().appName("Test Odps Read")
    .config("spark.sql.codegen.wholeStage", false)
    .master("local[*]").getOrCreate()
  val odpsOps = new OdpsOps(ss.sparkContext, accessKeyId, accessKeySecret,
    urls(envType)(0), urls(envType)(1))
  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  override def beforeAll(): Unit = {
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0), urls(envType)(1))
    odpsUtils.runSQL(project,
      // scalastyle:off
      """
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
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0), urls(envType)(1))
    odpsUtils.runSQL(project, "TRUNCATE TABLE odps_basic_types;")
  }

  test("[OdpsOpsSuite] support basic types") {
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
        StructField("o", StructType(Array(StructField("s1", DoubleType), StructField("s2", TimestampType))))
        :: Nil)

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm", Locale.US)
    val dataSeq = Array(
      (false, 11.toShort, 12, 13L, 14.0f, 15.0d, new java.math.BigDecimal("3.520122999999999891"),
        new Date(dateFormat.parse("01/11/2015 18:00").getTime),
        new Timestamp(1510429612345L), "test-string", "16".toByte, testBytes,
        Array(1.1, 2.2).toSeq,
        Map(1.2 -> new Timestamp(1510429612346L)),
        Row(1.2, new Timestamp(1510429612347L))
      ))

    val data = ss.sparkContext.parallelize(dataSeq, 2)
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0), urls(envType)(1))
    odpsUtils.runSQL(project, s"TRUNCATE TABLE $table;")
    odpsOps.saveToTable(project, table, data, OdpsOpsSuite.writeTransfer)

    val df = odpsOps.loadOdpsTable(ss.sqlContext, project, table, new Array[Int](0), 2)

    assert(df.schema.fieldNames === struct.fieldNames)
    val collectList = df.collect()
    dataSeq.zip(collectList).foreach( e => {
      Row.fromTuple(e._1).toSeq.zip(e._2.toSeq).foreach( t => {
        if (t._1.isInstanceOf[Array[Byte]] && t._2.isInstanceOf[Array[Byte]]) {
          t._1.asInstanceOf[Array[Byte]].deep == t._2.asInstanceOf[Array[Byte]].deep
        } else {
          t._1.toString == t._2.toString
        }
      })
    })
  }
}

object OdpsOpsSuite {
  def writeTransfer(
      tuple: (Boolean, Short, Int, Long, Float, Double, java.math.BigDecimal,
        Date, Timestamp, String, Byte, Array[Byte], Seq[Double], Map[Double, Timestamp], Row),
      emptyReord: Record,
      schema: TableSchema): Unit = {
    emptyReord.set(0, tuple._1)
    emptyReord.set(1, tuple._2)
    emptyReord.set(2, tuple._3)
    emptyReord.set(3, tuple._4)
    emptyReord.set(4, tuple._5)
    emptyReord.set(5, tuple._6)
    emptyReord.set(6, tuple._7)
    emptyReord.set(7, tuple._8)
    emptyReord.set(8, tuple._9)
    emptyReord.set(9, tuple._10)
    emptyReord.set(10, tuple._11)
    emptyReord.set(11, new Binary(tuple._12))
    emptyReord.set(12, OdpsUtils.sparkData2OdpsData(schema.getColumn(12).getTypeInfo)(tuple._13))
    emptyReord.set(13, OdpsUtils.sparkData2OdpsData(schema.getColumn(13).getTypeInfo)(tuple._14))
    emptyReord.set(14, OdpsUtils.sparkData2OdpsData(schema.getColumn(14).getTypeInfo)(tuple._15))
  }
}
