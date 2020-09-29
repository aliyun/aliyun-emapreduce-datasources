/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.aliyun.tablestore

import java.util.Locale

import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class TableStoreSourceProvider
    extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with StreamSourceProvider
    with StreamSinkProvider
    with Logging {
  override def shortName(): String = "tablestore"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new TableStoreSink(parameters, Some(TableStoreCatalog(parameters).schema))(sqlContext)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (shortName(), TableStoreSource.tableStoreSchema(TableStoreCatalog(parameters).schema))
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    validateOptions(parameters, isStream = true)
    val caseInsensitiveParams = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }
    val tableStoreOffsetReader = new TableStoreOffsetReader(caseInsensitiveParams)
    new TableStoreSource(
      sqlContext,
      schema,
      tableStoreOffsetReader,
      caseInsensitiveParams,
      metadataPath
    )
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    validateOptions(parameters, isStream = false)
    new TableStoreRelation(parameters, Some(schema))(sqlContext)
  }

  override def createRelation(
     sqlContext: SQLContext,
     parameters: Map[String, String]): BaseRelation = {
    validateOptions(parameters, isStream = false)
    new TableStoreRelation(parameters, Some(TableStoreCatalog(parameters).schema))(sqlContext)
  }

  def validateOptions(caseInsensitiveParams: Map[String, String], isStream: Boolean): Unit = {
    caseInsensitiveParams.getOrElse(
      "table.name",
      throw new MissingArgumentException("Missing TableStore table (='table.name').")
    )
    caseInsensitiveParams.getOrElse(
      "instance.name",
      throw new MissingArgumentException("Missing TableStore table (='instance.name').")
    )
    if (isStream) {
      caseInsensitiveParams.getOrElse(
        "tunnel.id",
        throw new MissingArgumentException("Missing TableStore tunnel (='tunnel.id').")
      )
    }
    caseInsensitiveParams.getOrElse(
      "access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id').")
    )
    caseInsensitiveParams.getOrElse(
      "access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret').")
    )
    caseInsensitiveParams.getOrElse(
      "endpoint",
      throw new MissingArgumentException("Missing log store endpoint (='endpoint').")
    )
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(s"Save mode $mode not allowed for tablestore. " +
          s"Allowed save modes are ${SaveMode.Append} and ${SaveMode.ErrorIfExists} (default).")
      case _ => // ok
    }

    new TableStoreWriter()
      .write(parameters, sqlContext.sparkSession, data.queryExecution, parameters)

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for OTS. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from OTS write " +
          "operation is not usable.")
    }
  }
}

object TableStoreSourceProvider extends Logging {
  val MAX_OFFSETS_PER_TRIGGER = "maxoffsetspertrigger"
  val MAX_OFFSETS_PER_CHANNEL = "maxoffsetsperchannel"

  val TUNNEL_CLIENT_TAG = "spark-client"
  val OTS_CHANNEL_FINISHED = "finished"

  val __OTS_RECORD_TYPE__ = "__ots_record_type__"
  val __OTS_RECORD_TIMESTAMP__ = "__ots_record_timestamp__"
  val __OTS_COLUMN_TYPE_PREFIX = "__ots_column_type_"

  def isDefaultField(fieldName: String): Boolean = {
    fieldName == __OTS_RECORD_TYPE__ ||
    fieldName == __OTS_RECORD_TIMESTAMP__
  }
}
