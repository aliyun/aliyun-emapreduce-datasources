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

package org.apache.spark.sql.aliyun.datahub

import java.util.{Locale, Optional, UUID}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader}
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, MicroBatchReadSupport, StreamWriteSupport}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class DatahubSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with MicroBatchReadSupport
  with ContinuousReadSupport
  with StreamWriteSupport{
  override def shortName(): String = "datahub"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (shortName(), DatahubSchema.getSchema(schema, parameters))
  }

  @deprecated("use DataSourceV2 impl", "1.8.0")
  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String, 
      schema: Option[StructType], 
      providerName: String, 
      parameters: Map[String, String]): Source = {
    new DatahubSource(sqlContext, schema, parameters, metadataPath,
      new DatahubOffsetReader(parameters), getStartOffset(parameters))
  }

  private def getStartOffset(parameters: Map[String, String]): DatahubOffsetRangeLimit = {
    parameters.get("start.offset") match {
      case Some(offset) if offset == "latest" => LatestOffsetRangeLimit
      case Some(offset) if offset == "oldest" => OldestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(DatahubSourceOffset.partitionOffsets(json))
      case None => LatestOffsetRangeLimit
    }
  }

  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = {
    val parameters = options.asMap().asScala.toMap
    val uniqueGroupId = s"spark-datahub-source-${UUID.randomUUID}-${checkpointLocation.hashCode}"
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val startingStreamOffset = DatahubOffsetRangeLimit.getOffsetRangeLimit(caseInsensitiveParams,
      DatahubOffsetRangeLimit.STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    val datahubOffsetReader = new DatahubOffsetReader(caseInsensitiveParams)
    new DatahubContinuousReader(
      Some(schema.orElse(new StructType())),
      datahubOffsetReader,
      paramsForExecutors(parameters, uniqueGroupId),
      parameters,
      checkpointLocation,
      startingStreamOffset)
  }

  private def paramsForExecutors(
      specifiedDatahubParams: Map[String, String],
      uniqueGroupId: String): java.util.Map[String, Object] =
    ConfigUpdater("executor", specifiedDatahubParams)
      .build()

  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): MicroBatchReader = {
    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val datahubOffsetReader = new DatahubOffsetReader(caseInsensitiveParams)
    val startingStreamOffsets = DatahubOffsetRangeLimit.getOffsetRangeLimit(caseInsensitiveParams,
      "startingoffsets", LatestOffsetRangeLimit)

    new DatahubMicroBatchReader(
      datahubOffsetReader,
      options,
      checkpointLocation,
      startingStreamOffsets,
      caseInsensitiveParams.getOrElse("failondataloss", "true").toBoolean,
      Some(schema.orElse(new StructType()).toDDL))
  }

  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    new DatahubStreamWriter()
  }
}

object DatahubSourceProvider {
  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE =
    """
      |Some data may have been lost because they are not available in Datahub any more; either the
      | data was aged out by Datahub or shard was in 'CLOSED' status and recycled. If you want your
      | streaming query to fail on such cases, set the source option "failOnDataLoss" to "true".
    """.stripMargin

  val INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE =
    """
      |Some data may have been lost because they are not available in Datahub any more; either the
      | data was aged out by Datahub or shard was in 'CLOSED' status and recycled. If you don't want
      | your streaming query to fail on such cases, set the source option "failOnDataLoss" to "false".
    """.stripMargin
}

/** Class to conveniently update Datahub config params, while logging the changes */
private case class ConfigUpdater(module: String, datahubParams: Map[String, String]) extends Logging {
  private val map = new java.util.HashMap[String, Object](datahubParams.asJava)

  def set(key: String, value: Object): this.type = {
    map.put(key, value)
    logDebug(s"$module: Set $key to $value, earlier value: ${datahubParams.getOrElse(key, "")}")
    this
  }

  def setIfUnset(key: String, value: Object): ConfigUpdater = {
    if (!map.containsKey(key)) {
      map.put(key, value)
      logDebug(s"$module: Set $key to $value")
    }
    this
  }

  def build(): java.util.Map[String, Object] = map
}