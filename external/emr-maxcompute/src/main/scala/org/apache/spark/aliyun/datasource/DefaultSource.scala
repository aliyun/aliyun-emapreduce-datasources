/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
