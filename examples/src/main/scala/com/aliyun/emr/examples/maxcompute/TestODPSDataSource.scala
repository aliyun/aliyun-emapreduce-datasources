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
package com.aliyun.emr.examples.maxcompute

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.sql.Date

object TestODPSDataSource {
  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: TestOdps <accessKeyId> <accessKeySecret> <envType> <project> <table> <numPartitions>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    envType          0 or 1
          |                     0: Public environment.
          |                     1: Aliyun internal environment, i.e. Aliyun ECS etc.
          |    project          Aliyun ODPS project
          |    table            Aliyun ODPS table
          |    numPartitions    the number of RDD partitions
        """.stripMargin)
      System.exit(1)
    }

    val accessKeyId = args(0)
    val accessKeySecret = args(1)
    val envType = args(2).toInt
    val project = args(3)
    val table = args(4)
    val numPartitions = args(5).toInt

    val urls = Seq(
      Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"), // public environment
      Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com") // Aliyun internal environment
    )

    val odpsUrl = urls(envType)(0)
    val tunnelUrl = urls(envType)(1)

    val conf = new SparkConf().setAppName("Test Odps Read")
    val ss = SparkSession.builder().appName("Test Odps Read").master("local").getOrCreate()

    import ss.implicits._

    val dataSeq = (1 to 1000000).map {
      index => (index, (index-3).toString)
    }.toSeq


    val df = ss.sparkContext.makeRDD(dataSeq).toDF("a", "b")

    System.out.println("*****" + table + ",before overwrite table")
    df.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", odpsUrl)
      .option("tunnelUrl", tunnelUrl)
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).mode(SaveMode.Overwrite).save()

    System.out.println("*****" + table + ",after overwrite table, before read table")

    val readDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl", odpsUrl)
      .option("tunnelUrl", tunnelUrl)
      .option("table", table)
      .option("project", project)
      .option("accessKeySecret", accessKeySecret)
      .option("accessKeyId", accessKeyId).load()


    val collectList = readDF.collect()
    System.out.println("*****" + table + ",after read table," + collectList.size)
    assert(collectList.length == 1000000)
    assert((1 to 1000000).par.exists(n => collectList.exists(_.getLong(0) == n)))

  }
}
