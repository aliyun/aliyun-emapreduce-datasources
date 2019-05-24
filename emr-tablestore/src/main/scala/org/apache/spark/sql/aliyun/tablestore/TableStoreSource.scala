package org.apache.spark.sql.aliyun.tablestore

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType

class TableStoreSource(
    parameters: Map[String, String],
    userSpecifiedschema: Option[StructType]) extends Source with Logging {
  override def schema: StructType = userSpecifiedschema.getOrElse(new StructType)

  override def getOffset: Option[Offset] = {
  throw new UnsupportedOperationException("unsupport tablestore datasource.")
}

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
  throw new UnsupportedOperationException("unsupport tablestore datasource.")
}

  override def stop(): Unit = {}
}
