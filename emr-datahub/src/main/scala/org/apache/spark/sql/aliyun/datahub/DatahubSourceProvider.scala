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
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.aliyun.datahub.client.{DatahubClient, DatahubClientBuilder}
import com.aliyun.datahub.client.auth.AliyunAccount
import com.aliyun.datahub.client.common.DatahubConfig
import com.aliyun.datahub.client.http.HttpConfig
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class DatahubSourceProvider extends DataSourceRegister
  with MicroBatchReadSupport
  with ContinuousReadSupport
  with StreamWriteSupport
  with CreatableRelationProvider
  with RelationProvider
  with SchemaRelationProvider {
  override def shortName(): String = "datahub"

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
    val opts = options.asMap().asScala.toMap
    val project = opts.get(DatahubSourceProvider.OPTION_KEY_PROJECT).map(_.trim)
    val topic = opts.get(DatahubSourceProvider.OPTION_KEY_TOPIC).map(_.trim)
    new DatahubStreamWriter(project, topic, opts, None)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    new DatahubRelation(sqlContext, parameters, Some(schema))
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    new DatahubRelation(sqlContext, parameters, None)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(s"Save mode $mode not allowed for datahub. " +
          s"Allowed save modes are ${SaveMode.Append} and ${SaveMode.ErrorIfExists} (default).")
      case _ => // ok
    }

    val project = parameters.get(DatahubSourceProvider.OPTION_KEY_PROJECT).map(_.trim)
    val topic = parameters.get(DatahubSourceProvider.OPTION_KEY_TOPIC).map(_.trim)
    data.foreachPartition { it =>
      val writer = new DatahubWriter(project, topic, parameters, None)
        .createWriterFactory().createDataWriter(-1, -1, -1)
      it.foreach(t => writer.write(t.asInstanceOf[InternalRow]))
    }

    /* This method is suppose to return a relation that reads the data that was written.
     * We cannot support this for Datahub. Therefore, in order to make things consistent,
     * we return an empty base relation.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException
      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from Datahub write " +
            "operation is not usable.")
    }
  }
}

object DatahubSourceProvider {

  val OPTION_KEY_PROJECT = "project"
  val OPTION_KEY_TOPIC = "topic"
  val OPTION_KEY_ACCESS_ID = "access.key.id"
  val OPTION_KEY_ACCESS_KEY = "access.key.secret"
  val OPTION_KEY_ENDPOINT = "endpoint"
  val OPTION_KEY_RETRIES = "retry"
  val OPTION_KEY_BATCH_SIZE = "batch.size"
  val OPTION_KEY_BATCH_NUM = "batch.number"

  // TODO: expose all the options for DatahubConfig & HttpConfig

  private[datahub] val datahubClientPool =
    new mutable.HashMap[(String, String), DatahubClient]()
  private val rwlock: ReadWriteLock = new ReentrantReadWriteLock()

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
      | your streaming query to fail on such cases, set the source option "failOnDataLoss" to
      | "false".
    """.stripMargin

  // Upgrade to the interfaces of Datahub Java SDK Version 2.12.2
  def getOrCreateDatahubClientV2(sourceOptions: Map[String, String]): DatahubClient = {
    val accessId = sourceOptions.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    val accessKey = sourceOptions.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    val endpoint = sourceOptions.getOrElse("endpoint",
      throw new MissingArgumentException("Missing endpoint (='endpoint')."))
    val retries = sourceOptions.getOrElse(OPTION_KEY_RETRIES, "3").toInt

    rwlock.readLock().lock()
    val client = datahubClientPool.get((accessId, endpoint))
    rwlock.readLock().unlock()

    client match {
      case Some(datahubClient) => datahubClient

      case None =>
        rwlock.writeLock().lock()
        val datahubConfig = new DatahubConfig(endpoint,
                                              new AliyunAccount(accessId, accessKey),
                                              true)
        val httpConfig: HttpConfig = new HttpConfig()
        httpConfig.setMaxRetryCount(3)

        val datahubClient = DatahubClientBuilder.newBuilder()
          .setDatahubConfig(datahubConfig)
          .setHttpConfig(httpConfig)
          .build()

        datahubClientPool.put((accessId, endpoint), datahubClient)
        rwlock.writeLock().unlock()
        datahubClient
    }
  }
}

/** Class to conveniently update Datahub config params, while logging the changes */
private case class ConfigUpdater(module: String, datahubParams: Map[String, String])
  extends Logging {
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
