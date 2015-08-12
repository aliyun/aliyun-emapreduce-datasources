package org.apache.spark.aliyun.oss

import org.apache.spark.{LocalSparkContext, SparkContext, SparkConf}
import org.scalatest.FunSuite

class OssRDDSuite extends FunSuite with LocalSparkContext {
  private val accessKeyId: String = ""
  private val accessKeySecret: String = ""
  private val endpoint = ""
  private val filePath = ""

  test("OSS file r/w") {
    val conf = new SparkConf().setAppName("OSS file r/w").setMaster("local[4]")
    conf.set("spark.hadoop.fs.oss.accessKeyId", accessKeyId)
    conf.set("spark.hadoop.fs.oss.accessKeySecret", accessKeySecret)
    conf.set("spark.hadoop.fs.oss.endpoint", endpoint)
    sc = new SparkContext(conf)

    val data = sc.parallelize(1 to 30, 1)

    OssOps(sc, endpoint, accessKeyId, accessKeySecret)
      .saveToOssFile(filePath, data)

    val ossRDD = OssOps(sc, endpoint, accessKeyId, accessKeySecret)
                .readOssFile(filePath, 2)

    assert(ossRDD.count() == 30)
    assert(ossRDD.distinct().count() == 30)
  }
}
