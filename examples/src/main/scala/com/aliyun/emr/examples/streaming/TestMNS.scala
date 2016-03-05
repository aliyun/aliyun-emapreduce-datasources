package com.aliyun.emr.examples.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.mns.MnsUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Created by genmao.ygm on 2016/3/5.
 */
object TestMNS {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        """Usage: TestLoghub <queuename> <accessKeyId> <accessKeySecret> <endpoint>""".stripMargin)
      System.exit(1)
    }
    val queuename = args(0)
    val accessKeyId = args(1)
    val accessKeySecret = args(2)
    val endpoint = args(3)

    val conf = new SparkConf().setAppName("Test MNS")
    val batchInterval = Seconds(10)
    val ssc = new StreamingContext(conf, batchInterval)

    val mnsStream = MnsUtils.createPullingStreamAsBytes(ssc, queuename, accessKeyId, accessKeySecret, endpoint,
      StorageLevel.MEMORY_ONLY)
    mnsStream.foreachRDD( rdd => {
      rdd.collect().foreach(e => println(new String(e)))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
