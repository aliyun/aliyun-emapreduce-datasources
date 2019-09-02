package com.aliyun.emr.examples.streaming


import com.aliyun.openservices.aliyun.log.producer.{Callback, Result}
import com.aliyun.openservices.log.common.LogItem
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.aliyun.logservice.writer._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object TestLoghubWriter {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: TestDirectLoghub <sls project> <sls logstore> <loghub group name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds> <zookeeper host:port=localhost:2181>
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
    val zkAddress = if (args.length >= 8) args(7) else "localhost:2181"
    val targetLogstore = "test-logstore"

    val conf = new SparkConf().setAppName("Test Direct SLS Loghub")
      .setMaster("local[2]")
      .set("spark.streaming.loghub.maxRatePerShard", "10")
      .set("spark.loghub.batchGet.step", "1")
    val zkParas = Map("zookeeper.connect" -> zkAddress,
      "enable.auto.commit" -> "false")
    val ssc = new StreamingContext(conf, batchInterval)

    val loghubStream = LoghubUtils.createDirectStream(
      ssc,
      loghubProject,
      logStore,
      loghubGroupName,
      accessKeyId,
      accessKeySecret,
      endpoint,
      zkParas,
      LogHubCursorPosition.BEGIN_CURSOR)

    val producerConfig = Map(
      "sls.project" -> loghubProject,
      "sls.logstore" -> targetLogstore,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "sls.endpoint" -> endpoint,
      "sls.ioThreadCount" -> "2"
    )

    val lines = loghubStream.map(x => x)

    def transformFunc(x: String): LogItem = {
      val r = new LogItem()
      r.PushBack("key", x)
      r
    }

    val callback = new Callback with Serializable {
      override def onCompletion(result: Result): Unit = {
        println(s"Send result ${result.isSuccessful}")
      }
    }

    lines.writeToLoghub(
      producerConfig,
      "topic",
      "streaming",
      transformFunc, Option.apply(callback))

    ssc.checkpoint("/tmp/spark5/streaming") // set checkpoint directory
    ssc.start()
    ssc.awaitTermination()
  }
}
