package org.apache.spark.aliyun.maxcompute.datasource

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{CreatableRelationProvider, BaseRelation, DataSourceRegister, RelationProvider}
import org.slf4j.LoggerFactory

/**
 * Created by songjun on 16/12/21.
 */
class DefaultSource extends RelationProvider
  with CreatableRelationProvider
  with DataSourceRegister {
  private val log = LoggerFactory.getLogger(getClass)

  override def shortName(): String = "odps"

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    val odpsOptions = new ODPSOptions(parameters)
    new ODPSRelation(odpsOptions.accessKeyId,
      odpsOptions.accessKeySecret,
      odpsOptions.odpsUrl,
      odpsOptions.tunnelUrl,
      odpsOptions.project,
      odpsOptions.table,
      odpsOptions.partitionSpec,
      odpsOptions.numPartitions)(sqlContext)
  }

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to Redshift
   */
  override def createRelation(
    sqlContext: SQLContext,
    saveMode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame): BaseRelation = {
    val odpsOptions = new ODPSOptions(parameters)

    new ODPSWriter(
      odpsOptions.accessKeyId,
      odpsOptions.accessKeySecret,
      odpsOptions.odpsUrl,
      odpsOptions.tunnelUrl
    ).saveToTable(odpsOptions.project, odpsOptions.table, data, odpsOptions.partitionSpec, odpsOptions.allowCreatNewPartition, saveMode)
    createRelation(sqlContext, parameters)
  }
}
