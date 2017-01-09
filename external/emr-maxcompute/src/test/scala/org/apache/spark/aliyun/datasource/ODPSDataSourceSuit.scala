package org.apache.spark.aliyun.datasource

import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.FunSuite

/**
 * Created by songjun on 17/1/9.
 */
class ODPSDataSourceSuit extends FunSuite {

  val accessKeyId = ""
  val accessKeySecret = ""
  val envType = 0
  val project = "test_odpss"
  val numPartitions = 2

  val urls = Seq(
    Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"), // public environment
    Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com") // Aliyun internal environment
  )

  val conf = new SparkConf().setAppName("Test Odps Read").setMaster("local")
  val ss = SparkSession.builder().appName("Test Odps Read").master("local").getOrCreate()

  import ss.implicits._

  test("write/read DataFrame to/from no-partition odps table should be ok") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    System.out.println("*****" + table + ",before overwrite table")
    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    System.out.println("*****" + table + ",after overwrite table, before read table")

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()


    val collectList = readDF.collect()
    System.out.println("*****" + table + ",after read table," + collectList.size)
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getLong(0) == n)))
  }

  test("write/read DataFrame to/from partition odps table should be ok") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Overwrite).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").load()

    val collectList = readDF.collect()
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getLong(0) == n)))

  }

  test("write DataFrame to no-partition odps table with SaveMode.Append should be ok") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    //First,Overwrite
    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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

    df1.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Append).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()

    val collectList = readDF.collect()
    assert(collectList.length == 52)
    assert((1 to 52).forall(n => collectList.exists(_.getLong(0) == n)))
  }


  test("write DataFrame to partition odps table with SaveMode.Append should be ok") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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

    df1.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Append).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").load()

    val collectList = readDF.collect()
    assert(collectList.length == 52)
    assert((1 to 52).forall(n => collectList.exists(_.getLong(0) == n)))

  }

  test("write DataFrame to no-partition odps table with SaveMode.ErrorIfExists should throw an Exception") {
    val table = "odps_no_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    //First,Overwrite
    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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
      df1.write.format("org.apache.spark.aliyun.maxcompute.datasource")
        .option("odpsUrl", "http://service.odps.aliyun.com/api")
        .option("tunnelUrl", "http://dt.odps.aliyun.com")
        .option("table", table)
        .option("project", project)
        .option("accessKeySecret", accessKeySecret)
        .option("accessKeyId", accessKeyId).mode(SaveMode.ErrorIfExists).save()

      assert(false)
    } catch {
      case e: Exception =>
        System.out.println("write DataFrame to no-partition odps table with SaveMode.ErrorIfExists should throw an Exception:" + e)
        assert(true)
    }

  }

  test("write DataFrame to partition odps table with SaveMode.ErrorIfExists should throw an Exception") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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
      df1.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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
        System.out.println("write DataFrame to partition odps table with SaveMode.ErrorIfExists should throw an Exception:" + e)
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
    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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

    df1.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Ignore).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()

    val collectList = readDF.collect()
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getLong(0) == n)))
  }


  test("write DataFrame to partition odps table with SaveMode.Ignore should be ok") {
    val table = "odps_partition_table"

    val dataSeq = ('a' to 'z').zipWithIndex.map {
      case (c, index) => (index + 1, c.toString)
    }.toSeq

    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
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

    df1.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").mode(SaveMode.Ignore).save()

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", "http://service.odps.aliyun.com/api")
      .option("tunnelUrl", "http://dt.odps.aliyun.com")
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId)
      .option("partitionSpec", "c='p1'").load()

    val collectList = readDF.collect()
    assert(collectList.length == 26)
    assert((1 to 26).forall(n => collectList.exists(_.getLong(0) == n)))

  }

}
