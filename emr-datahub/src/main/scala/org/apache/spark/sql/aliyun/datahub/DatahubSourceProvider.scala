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

import java.util
import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.aliyun.datahub.client.{DatahubClient, DatahubClientBuilder}
import com.aliyun.datahub.client.auth.AliyunAccount
import com.aliyun.datahub.client.common.DatahubConfig
import com.aliyun.datahub.client.http.HttpConfig
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.aliyun.datahub.DatahubOffsetRangeLimit.{ENDING_OFFSETS_OPTION_KEY, STARTING_OFFSETS_OPTION_KEY}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, PhysicalWriteInfoImpl, WriteBuilder}
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DatahubSourceProvider extends DataSourceRegister
  with TableProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with RelationProvider {

  override def shortName(): String = "datahub"

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
    data.foreachPartition { it: Iterator[Row] =>
      val writer = new DatahubWriter(project, topic, parameters, None)
        .createBatchWriterFactory(
          PhysicalWriteInfoImpl(data.rdd.asInstanceOf[RDD[Row]].getNumPartitions))
        .createWriter(-1, -1)
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

  class DatahubTable(userSpecifiedSchema: Option[StructType])
    extends Table with SupportsRead with SupportsWrite {

    override def name(): String = "DatahubTable"

    override def schema(): StructType = userSpecifiedSchema.getOrElse(new StructType())

    override def capabilities(): util.Set[TableCapability] = {
      import TableCapability._
      Set(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, CONTINUOUS_READ, STREAMING_WRITE).asJava
    }

    override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
      () => new DatahubScan(schema(), caseInsensitiveStringMap)
    }

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      new WriteBuilder {
        private val options = info.options
        private val inputSchema: StructType = info.schema()
        private val project: String = options.get(DatahubSourceProvider.OPTION_KEY_PROJECT)
        private val topic: String = options.get(DatahubSourceProvider.OPTION_KEY_TOPIC)

        override def buildForBatch(): BatchWrite = {
          assert(inputSchema != null)
          new DatahubWriter(Some(project), Some(topic), options.asScala.toMap, Some(inputSchema))
        }

        override def buildForStreaming(): StreamingWrite = {
          assert(inputSchema != null)
          new DatahubStreamWriter(
            Some(project),
            Some(topic),
            options.asScala.toMap,
            Some(inputSchema))
        }
      }
    }
  }

  class DatahubScan(
      schema: StructType,
      options: CaseInsensitiveStringMap) extends Scan {

    override def readSchema(): StructType = schema

    override def toBatch(): Batch = {
      val caseInsensitiveOptions = CaseInsensitiveMap(options.asScala.toMap)
      val datahubOffsetReader = new DatahubOffsetReader(caseInsensitiveOptions)

      val startingRelationOffsets = DatahubOffsetRangeLimit.getOffsetRangeLimit(
        caseInsensitiveOptions, STARTING_OFFSETS_OPTION_KEY, OldestOffsetRangeLimit)
      assert(startingRelationOffsets != LatestOffsetRangeLimit)

      val endingRelationOffsets = DatahubOffsetRangeLimit.getOffsetRangeLimit(
        caseInsensitiveOptions, ENDING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
      assert(endingRelationOffsets != OldestOffsetRangeLimit)

      new DatahubBatch(
        datahubOffsetReader,
        options,
        startingRelationOffsets,
        endingRelationOffsets,
        caseInsensitiveOptions.getOrElse("failondataloss", "true").toBoolean,
        Some(schema.toDDL))
    }

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      val caseInsensitiveOptions = CaseInsensitiveMap(options.asScala.toMap)
      val datahubOffsetReader = new DatahubOffsetReader(caseInsensitiveOptions)
      val startingStreamOffsets = DatahubOffsetRangeLimit.getOffsetRangeLimit(
        caseInsensitiveOptions,
        "startingoffsets",
        LatestOffsetRangeLimit)

      new DatahubMicroBatchStream(
        datahubOffsetReader,
        options,
        checkpointLocation,
        startingStreamOffsets,
        caseInsensitiveOptions.getOrElse("failondataloss", "true").toBoolean,
        Some(schema.toDDL))
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      val caseInsensitiveOptions = CaseInsensitiveMap(options.asScala.toMap)
      val startingStreamOffset = DatahubOffsetRangeLimit.getOffsetRangeLimit(caseInsensitiveOptions,
        DatahubOffsetRangeLimit.STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
      val datahubOffsetReader = new DatahubOffsetReader(caseInsensitiveOptions)

      new DatahubContinuousStream(
        Some(schema),
        datahubOffsetReader,
        ConfigUpdater("executor", caseInsensitiveOptions).build(),
        caseInsensitiveOptions,
        checkpointLocation,
        startingStreamOffset)
    }
  }

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    null
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    assert(partitioning.isEmpty)
    new DatahubTable(Option(schema))
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
