package org.apache.spark.sql.aliyun.jdbc2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType

class JdbcSource(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    userSpecifiedschema: Option[StructType]) extends Source with Logging {
  val jdbcOptions = new JDBCOptions(parameters)
  val resolver = sqlContext.conf.resolver

  override def schema: StructType =
    userSpecifiedschema.getOrElse(JDBCRelation.getSchema(resolver, jdbcOptions))

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException("unsupport jdbc datasource.")
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    throw new UnsupportedOperationException("unsupport jdbc datasource.")
  }

  override def stop(): Unit = {}
}

