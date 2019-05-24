package org.apache.spark.sql.aliyun.hbase

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType

class HBaseSource(
    parameters: Map[String, String],
    userSpecifiedschema: Option[StructType]) extends Source with Logging {
  val catalog = HBaseTableCatalog(parameters)

  override def schema: StructType = userSpecifiedschema.getOrElse(catalog.toDataType)

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException("unsupport hbase datasource.")
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    throw new UnsupportedOperationException("unsupport hbase datasource.")
  }

  override def stop(): Unit = {}
}
