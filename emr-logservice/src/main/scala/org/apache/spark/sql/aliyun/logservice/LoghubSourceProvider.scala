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
package org.apache.spark.sql.aliyun.logservice

import java.util.{Locale, Optional, UUID}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.aliyun.loghub.LoghubSink
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions}
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class LoghubSourceProvider extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with SchemaRelationProvider
    with RelationProvider
    with CreatableRelationProvider
    with ContinuousReadSupport
    with Logging {
  import LoghubSourceProvider._

  override def shortName(): String = "loghub"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (shortName(), schema.getOrElse({
      logInfo(s"Using default schema: ${LoghubSourceProvider.getDefaultSchema}")
      LoghubSourceProvider.getDefaultSchema
    }))
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    Utils.validateOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val startingStreamOffsets = LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY,
      LatestOffsetRangeLimit)
    val loghubOffsetReader = new LoghubOffsetReader(caseInsensitiveParams)
    val _schema = schema.getOrElse({
      logInfo(s"Using default schema: ${LoghubSourceProvider.getDefaultSchema}")
      LoghubSourceProvider.getDefaultSchema
    })
    new LoghubSource(
      sqlContext,
      _schema,
      LoghubSourceProvider.isDefaultSchema(_schema),
      parameters,
      metadataPath,
      startingStreamOffsets,
      loghubOffsetReader)
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new LoghubSink(sqlContext, parameters)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    validateBatchOptions(parameters)
    require(schema.nonEmpty, "Unable to infer the schema. The schema specification " +
      "is required to create the table.;")

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    val startingRelationOffsets = LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams,
      ENDING_OFFSETS_OPTION_KEY,
      LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)

    new LoghubRelation(
      sqlContext,
      schema,
      LoghubSourceProvider.isDefaultSchema(schema),
      parameters,
      startingRelationOffsets,
      endingRelationOffsets)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    validateBatchOptions(parameters)

    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    val startingRelationOffsets = LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit)
    assert(startingRelationOffsets != LatestOffsetRangeLimit)

    val endingRelationOffsets = LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams,
      ENDING_OFFSETS_OPTION_KEY,
      LatestOffsetRangeLimit)
    assert(endingRelationOffsets != EarliestOffsetRangeLimit)

    val schema = LoghubSourceProvider.getDefaultSchema
    new LoghubRelation(
      sqlContext,
      schema,
      true,
      parameters,
      startingRelationOffsets,
      endingRelationOffsets)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(s"Save mode $mode not allowed for Loghub. " +
          s"Allowed save modes are ${SaveMode.Append} and " +
          s"${SaveMode.ErrorIfExists} (default).")
      case _ => // ok
    }

    LoghubWriter.write(sqlContext.sparkSession, data.queryExecution, parameters)

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for Loghub. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from Loghub write " +
          "operation is not usable.")
    }
  }

  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = {
    val parameters = options.asMap().asScala.toMap
    val specifiedLoghubParams =
      parameters
        .keySet
        .filter(_.toLowerCase(Locale.ROOT).startsWith("loghub."))
        .map { k => k.drop(6).toString -> parameters(k) }
        .toMap
    val uniqueGroupId = s"spark-loghub-source-${UUID.randomUUID}-${checkpointLocation.hashCode}"
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val startingStreamOffset = LoghubSourceProvider.getLoghubOffsetRangeLimit(caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)

    val loghubOffsetReader = new LoghubOffsetReader(caseInsensitiveParams)

    val _schema = schema.orElse({
      logInfo(s"Using default schema: ${LoghubSourceProvider.getDefaultSchema}")
      LoghubSourceProvider.getDefaultSchema
    })
    new LoghubContinuousReader(
      _schema,
      LoghubSourceProvider.isDefaultSchema(_schema),
      loghubOffsetReader,
      paramsForExecutors(specifiedLoghubParams, uniqueGroupId),
      parameters,
      checkpointLocation,
      startingStreamOffset)
  }

  def paramsForExecutors(
      specifiedLoghubParams: Map[String, String],
      uniqueGroupId: String): java.util.Map[String, Object] =
    ConfigUpdater("executor", specifiedLoghubParams)
      .build()

  def validateBatchOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    Utils.validateOptions(caseInsensitiveParams)

    LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffsetRangeLimit) match {
      case EarliestOffsetRangeLimit => // good to go
      case LatestOffsetRangeLimit =>
        throw new IllegalArgumentException("starting offset can't be latest " +
          "for batch queries on Loghub")
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        partitionOffsets.foreach {
          case (shard, off) if off._1 == LoghubOffsetRangeLimit.LATEST =>
            throw new IllegalArgumentException(s"startingoffsets for $shard can't " +
              "be latest for batch queries on Loghub")
          case _ => // ignore
        }
    }

    LoghubSourceProvider.getLoghubOffsetRangeLimit(
      caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit) match {
      case EarliestOffsetRangeLimit =>
        throw new IllegalArgumentException("ending offset can't be earliest " +
          "for batch queries on Loghub")
      case LatestOffsetRangeLimit => // good to go
      case SpecificOffsetRangeLimit(partitionOffsets) =>
        partitionOffsets.foreach {
          case (shard, off) if off._1 == LoghubOffsetRangeLimit.EARLIEST =>
            throw new IllegalArgumentException(s"endingoffsets for $shard can't " +
              "be latest for batch queries on Loghub")
          case _ => // ignore
        }
    }
  }
}

object LoghubSourceProvider {

  val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"
  val __PROJECT__ = "__logProject__"
  val __STORE__ = "__logStore__"
  val __SHARD__ = "__shard__"
  val __TIME__ = "__time__"
  val __TOPIC__ = "__topic__"
  val __SOURCE__ = "__source__"
  val __VALUE__ = "__value__"
  val __SEQUENCE_NUMBER__ = "__sequence_number__"

  def getDefaultSchema: StructType = {
    new StructType()
      .add(StructField(__PROJECT__, StringType))
      .add(StructField(__STORE__, StringType))
      .add(StructField(__SHARD__, StringType))
      .add(StructField(__TIME__, StringType))
      .add(StructField(__TOPIC__, StringType))
      .add(StructField(__SOURCE__, StringType))
      .add(StructField(__VALUE__, StringType))
      .add(StructField(__SEQUENCE_NUMBER__, StringType))
  }

  def isDefaultSchema(schema: StructType): Boolean = {
    !(schema.fields.map(f => (f.name, f.dataType.simpleString))
      .zip(getDefaultSchema.fields.map(f => (f.name, f.dataType.simpleString)))
      .exists{ case (l, r) => !l._1.equals(r._1) || !l._2.equals(r._2)})
  }

  def getLoghubOffsetRangeLimit(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: LoghubOffsetRangeLimit): LoghubOffsetRangeLimit = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffsetRangeLimit
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        EarliestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(LoghubSourceOffset.partitionOffsets(json, params))
      case None => defaultOffsets
    }
  }
}

/** Class to conveniently update Loghub config params, while logging the changes */
private case class ConfigUpdater(module: String, loghubParams: Map[String, String])
  extends Logging {
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
