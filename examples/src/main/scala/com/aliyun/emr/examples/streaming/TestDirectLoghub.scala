package com.aliyun.emr.examples.streaming

import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.aliyun.logservice.{DirectLoghubInputDStream, LoghubUtils}

object TestDirectLoghub {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: TestLoghub <sls project> <sls logstore> <loghub group name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds>
        """.stripMargin)
      System.exit(1)
    }

    val loghubProject = args(0)
    val logStore = args(1)
    val loghubGroupName = args(2)
    val endpoint = args(3)
    val accessKeyId = args(4)
    val accessKeySecret = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)

    def functionToCreateContext(): StreamingContext = {
      val conf = new SparkConf().setAppName("Test SLS Loghub").setMaster("local[5]")
      val ssc = new StreamingContext(conf, batchInterval)
      ssc.sparkContext.setLogLevel("DEBUG")
      val zkParas = Map("zookeeper.connect" -> "114.55.55.15:2181",
        "enable.auto.commit" -> "false")
      val loghubStream = LoghubUtils.createDirectStream(
        ssc,
        loghubProject,
        logStore,
        "mugen",
        accessKeyId,
        accessKeySecret,
        endpoint,
        zkParas,
        LogHubCursorPosition.END_CURSOR)

      loghubStream.checkpoint(batchInterval).foreachRDD(rdd => {
        println(s"count by key: ${rdd.map(s => {
          s.sorted
          (s.length, s)
        }).countByKey().size}")
        loghubStream.asInstanceOf[DirectLoghubInputDStream].commitAsync()
      })
      ssc.checkpoint("file:///tmp/spark/streaming") // set checkpoint directory
      ssc
    }

    val ssc = StreamingContext.getOrCreate("file:///tmp/spark/streaming", functionToCreateContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
