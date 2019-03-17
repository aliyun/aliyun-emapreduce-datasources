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
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DatahubSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with ContinuousReadSupport{
  override def shortName(): String = "datahub"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (shortName(), DatahubSchema.getSchema(schema, parameters))
  }

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
      specifiedLoghubParams: Map[String, String],
      uniqueGroupId: String): java.util.Map[String, Object] =
    ConfigUpdater("executor", specifiedLoghubParams)
      .build()
}

/** Class to conveniently update Loghub config params, while logging the changes */
private case class ConfigUpdater(module: String, loghubParams: Map[String, String]) extends Logging {
  private val map = new java.util.HashMap[String, Object](loghubParams.asJava)

  def set(key: String, value: Object): this.type = {
    map.put(key, value)
    logDebug(s"$module: Set $key to $value, earlier value: ${loghubParams.getOrElse(key, "")}")
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