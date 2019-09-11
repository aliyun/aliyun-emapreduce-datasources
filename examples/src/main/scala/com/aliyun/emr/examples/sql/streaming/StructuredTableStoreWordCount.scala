package com.aliyun.emr.examples.sql.streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession

object StructuredTableStoreWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        "Usage: StructuredTableStoreWordCount <ots-instanceName>" +
          "<ots-tableName> <ots-tunnelId> <access-key-id> <access-key-secret> <ots-endpoint>" +
          "<max-offsets-per-channel> [<checkpoint-location>]"
      )
    }

    val Array(
      instanceName,
      tableName,
      tunnelId,
      accessKeyId,
      accessKeySecret,
      endpoint,
      maxOffsetsPerChannel,
      _*
    ) = args

    System.out.println(args.toSeq.toString)

    val checkpointLocation =
      if (args.length > 7) args(7)
      else "/tmp/temporary-" + UUID.randomUUID.toString

    val spark = SparkSession.builder
      .appName("TableStoreWordCount")
      .master("local[5]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.readStream
      .format("tablestore")
      .option("ots.instance", instanceName)
      .option("ots.table", tableName)
      .option("ots.tunnel", tunnelId)
      .option("endpoint", endpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("maxOffsetsPerChannel", maxOffsetsPerChannel)
      .option(
        "catalog",
        "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"int\"}," +
          "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}}}"
      )
      .load()
      .selectExpr("PkString", "PkInt", "__ots_record_type__")
      .as[(String, Integer, String)]

    println(lines.printSchema())
    val wordCounts = lines
      .flatMap(line => {
        System.out.println(s"wordCount line: ${line}")
        line._1.split(" ")
      })
      .groupBy("value")
      .count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
