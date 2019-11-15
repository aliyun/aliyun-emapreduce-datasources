/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.aliyun.odps.datasource

import com.aliyun.odps.{Column, OdpsType, TableSchema}
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ODPSDataSourceSuite extends SparkFunSuite {

  val accessKeyId: String = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
  val accessKeySecret: String = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")

  val envType: Int = {
    val envType = Option(System.getenv("TEST_ENV_TYPE")).getOrElse("public").toLowerCase
    if (envType != "private" && envType != "public") {
      throw new Exception(s"Unsupported test environment type: $envType, only support private or public")
    }
    if (envType.equals("public")) 0 else 1
  }
  // Update this with your own testing odps project.
  val project: String = Option(System.getenv("ODPS_PROJECT_NAME")).getOrElse("")
  val numPartitions = 2

  val urls = Seq(
    Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"), // public environment
    Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com") // Aliyun internal environment
  )

  val conf = new SparkConf().setAppName("Test Odps Read").setMaster("local[*]")
  val ss = SparkSession.builder().appName("Test Odps Read").master("local[*]").getOrCreate()

  override def beforeAll(): Unit = {
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0))
    val schema = new TableSchema
    schema.addColumn(new Column("a", OdpsType.INT))
    schema.addColumn(new Column("b", OdpsType.STRING))
    odpsUtils.createTable(project, "odps_no_partition_table", schema, true)
    schema.addPartitionColumn(new Column("c", OdpsType.STRING))
    odpsUtils.createTable(project, "odps_partition_table", schema, true)
  }

  override def afterAll(): Unit = {
    val odpsUtils = OdpsUtils(accessKeyId, accessKeySecret, urls(envType)(0))
    odpsUtils.runSQL(project, "TRUNCATE TABLE odps_no_partition_table;")
    odpsUtils.runSQL(project, "TRUNCATE TABLE odps_partition_table;")
  }

  import ss.implicits._

  test("write/read DataFrame to/from no-partition odps table should be ok") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    System.out.println("*****" + table + ",before overwrite table")
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    System.out.println("*****" + table + ",after overwrite table, before read table")

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()


    val collectList = readDF.collect()
    System.out.println("*****" + table + ",after read table," + collectList.size)
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getInt(0) == n)))
  }

  test("write/read DataFrame to/from partition odps table should be ok") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Overwrite).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").load()

    val collectList = readDF.collect()
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getInt(0) == n)))

  }

  test("write DataFrame to no-partition odps table with SaveMode.Append should be ok") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    //First,Overwrite
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    //Second,Append
    val dataSeq1 = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1 + 26, c.toString)
    }.toSeq
    val df1 = ss.sparkContext.makeRDD(dataSeq1).toDF("a", "b")

    df1.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Append).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()

    val collectList = readDF.collect()
    assert(collectList.length == 52)
    assert((1 to 52).forall(n => collectList.exists(_.getInt(0) == n)))
  }


  test("write DataFrame to partition odps table with SaveMode.Append should be ok") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Overwrite).save()

    val dataSeq1 = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1 + 26, c.toString)
    }.toSeq
    val df1 = ss.sparkContext.makeRDD(dataSeq1).toDF("a", "b")

    df1.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Append).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").load()

    val collectList = readDF.collect()
    assert(collectList.length == 52)
    assert((1 to 52).forall(n => collectList.exists(_.getInt(0) == n)))

  }

  test("write DataFrame to no-partition odps table with SaveMode.ErrorIfExists should throw an Exception") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    //First,Overwrite
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    val dataSeq1 = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1 + 26, c.toString)
    }.toSeq
    val df1 = ss.sparkContext.makeRDD(dataSeq1).toDF("a", "b")

    try {
      df1.write.format("org.apache.spark.aliyun.odps.datasource")
        .option("odpsUrl", "http://service.odps.aliyun.com/api")
        .option("tunnelUrl", "http://dt.odps.aliyun.com")
        .option("table", table)
        .option("project", project)
        .option("accessKeySecret", accessKeySecret)
        .option("accessKeyId", accessKeyId).mode(SaveMode.ErrorIfExists).save()

      assert(false)
    } catch {
      case e: Exception =>
        System.out.println("write DataFrame to no-partition odps table with " +
          "SaveMode.ErrorIfExists should throw an Exception:" + e)
        assert(true)
    }

  }

  test("write DataFrame to partition odps table with SaveMode.ErrorIfExists should throw an Exception") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Overwrite).save()

    val dataSeq1 = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1 + 26, c.toString)
    }.toSeq
    val df1 = ss.sparkContext.makeRDD(dataSeq1).toDF("a", "b")

    try {
      df1.write.format("org.apache.spark.aliyun.odps.datasource")
        .option("odpsUrl", "http://service.odps.aliyun.com/api")
        .option("tunnelUrl", "http://dt.odps.aliyun.com")
        .option("table", table)
        .option("project", project)
        .option("accessKeySecret", accessKeySecret)
        .option("accessKeyId", accessKeyId)
        .option("partitionSpec", "c='p1'").mode(SaveMode.ErrorIfExists).save()

      assert(false)
    } catch {
      case e: Exception =>
        System.out.println("write DataFrame to partition odps table with " +
          "SaveMode.ErrorIfExists should throw an Exception:" + e)
        assert(true)
    }

  }

  test("write DataFrame to no-partition odps table with SaveMode.Ignore should be ok") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    //First,Overwrite
    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    //Second,Append
    val dataSeq1 = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1 + 26, c.toString)
    }.toSeq
    val df1 = ss.sparkContext.makeRDD(dataSeq1).toDF("a", "b")

    df1.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Ignore).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()

    val collectList = readDF.collect()
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getInt(0) == n)))
  }


  test("write DataFrame to partition odps table with SaveMode.Ignore should be ok") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Overwrite).save()

    val dataSeq1 = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1 + 26, c.toString)
    }.toSeq
    val df1 = ss.sparkContext.makeRDD(dataSeq1).toDF("a", "b")

    df1.write.format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Ignore).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").load()

    val collectList = readDF.collect()
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getInt(0) == n)))
  }
}
