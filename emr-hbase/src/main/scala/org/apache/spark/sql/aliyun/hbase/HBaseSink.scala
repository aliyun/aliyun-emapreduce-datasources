package org.apache.spark.sql.aliyun.hbase

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink

class HBaseSink(sqlContext: SQLContext, sourceOptions: Map[String, String]) extends Sink with Logging {
  @volatile private var latestBatchId = -1L

  override def toString(): String = "HBaseSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      new HBaseWriter(sourceOptions).write(sqlContext.sparkSession, data.queryExecution, sourceOptions)
      latestBatchId = batchId
    }
  }
}
