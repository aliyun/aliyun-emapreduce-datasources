package org.apache.spark.sql.aliyun.hbase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.hbase.HBaseRelation
import org.apache.spark.sql.types.StructType

class HBaseWriter(parameters: Map[String, String]) {
  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      sourceOptions: Map[String, String]): Unit = {
    val schema = StructType.fromAttributes(queryExecution.analyzed.output)
    val relation = HBaseRelation(parameters, Some(schema))(sparkSession.sqlContext)
    relation.createTableIfNotExist()
    val encoder = RowEncoder(schema).resolveAndBind()
    val rdd = queryExecution.toRdd.map(r => encoder.fromRow(r))
    relation.insert(sparkSession.createDataFrame(rdd, schema), false)
  }
}
