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

package org.apache.spark.sql.aliyun.druid

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.metamx.common.Granularity
import com.metamx.tranquility.beam.{Beam, ClusteredBeamTuning}
import com.metamx.tranquility.druid.{DruidBeams, DruidLocation, DruidRollup, SpecificDruidDimensions}
import com.metamx.tranquility.spark.BeamFactory
import io.druid.data.input.impl.TimestampSpec
import io.druid.granularity.{QueryGranularities, QueryGranularity}
import io.druid.jackson.AggregatorsModule
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.joda.time.{DateTime, Period}

object DruidWriter {
  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      parameters: Map[String, String]): Unit = {
    val schema = getSchema(parameters)
    val timestampColumn = parameters.getOrElse("timestampSpec.column", "timestamp")
    import com.metamx.tranquility.spark.BeamRDD._
    queryExecution.toRdd.map(SchemaInternalRow(schema, _, timestampColumn)).propagate(new EventBeamFactory(parameters, schema))
  }

  def getSchema(parameters: Map[String, String]): StructType = {
    val dimension = parameters.getOrElse("rollup.dimensions",
      throw new AnalysisException(s"Option rollup.dimensions is required when create table without colunmn info. " +
        s"Format dimension1,dimension2...."))
    val dimensions = dimension.split(",")
    val timestampColumn = parameters.getOrElse("timestampSpec.column", "timestamp")
    val existTimestamp = dimensions.exists(_.equals(timestampColumn))
    if (!existTimestamp) {
      throw new AnalysisException("Missing timestamp column in option rollup.dimension")
    }
    StructType(dimensions.map(StructField(_, StringType)))
  }
}

class EventBeamFactory(druidConfiguration: Map[String, String], schema: StructType) extends BeamFactory[SchemaInternalRow]
{
  def makeBeam: Beam[SchemaInternalRow] = EventBeamFactory.BeamInstance(druidConfiguration, schema)
}

object EventBeamFactory extends Logging {
  private var curator: CuratorFramework = _
  private val mapper = new ObjectMapper()
  mapper.registerModule(new AggregatorsModule())

  def BeamInstance (
      druidConfiguration: Map[String, String],
      schema: StructType): Beam[SchemaInternalRow] = {
    val indexService = druidConfiguration.getOrElse("index.service",
      throw new AnalysisException(s"option index.service is required.")
    )
    val dataSource = druidConfiguration.getOrElse("data.source",
      throw new AnalysisException(s"option data.source is required.")
    )
    val firehouse = druidConfiguration.getOrElse("firehouse",
      throw new AnalysisException(s"option firehouse is required.")
    )
    val metricsSpec = druidConfiguration.getOrElse("rollup.aggregators",
      throw new AnalysisException(s"option metricsSpec is required. format: " +
        s"""{\\"metricsSpec\\":[{\\"type\\":\\"count\\",\\"name\\":\\"count\\"},{\\"type\\":\\"doubleSum\\",\\"fieldName\\":\\"x\\",\\"name\\":\\"x\\"}]}""")
    )
    val rollupQueryGranularities = druidConfiguration.getOrElse("rollup.query.granularities",
      throw new AnalysisException(s"option rollup.query.granularities is required.")
    )
    val discoveryPath = druidConfiguration.getOrElse("discovery.path", "/druid/discovery")
    val tuningSegmentGranularity = druidConfiguration
      .getOrElse("tuning.segment.granularity","DAY")
    val segmentGranularity = Granularity.valueOf(tuningSegmentGranularity.toUpperCase)
    val tuningWindowPeriod = druidConfiguration
      .getOrElse("tuning.window.period", "PT10M")
    val tuningPartitions = druidConfiguration
      .getOrElse("tuning.partitions", "1").toInt
    val tuningReplicants = druidConfiguration
      .getOrElse("tuning.replications", "1").toInt
    val tuningWarmingPeriod = druidConfiguration
      .getOrElse("tuning.warming.period", "0").toInt
    val warmingPeriod: Period = new Period(tuningWarmingPeriod)
    val timestampColumn = druidConfiguration.getOrElse("timestampSpec.column", "timestamp")
    val timestampFormat = druidConfiguration
      .getOrElse("timestampSpec.format", "iso")

    val aggregators = mapper.readValue(metricsSpec, classOf[AggregatorFactories])
    val dimensions = schema.fieldNames

    // TODO: CALENDRIC_GRANULARITIES
    val queryGranularities = rollupQueryGranularities.toLowerCase() match {
      case "all" => QueryGranularities.ALL
      case "none" => QueryGranularities.NONE
      case time =>  QueryGranularity.fromString(time)
    }

    val location = DruidLocation(indexService, firehouse, dataSource)
    if (curator == null) {
      curator = getCurator(druidConfiguration)
    }
    DruidBeams
      .builder((row: SchemaInternalRow) => {
        row.ts
      })
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(location)
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators.getAggregators, queryGranularities))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = segmentGranularity,
          windowPeriod = new Period(tuningWindowPeriod),
          partitions = tuningPartitions,
          replicants = tuningReplicants,
          warmingPeriod = warmingPeriod
        )
      )
      .timestampSpec(new TimestampSpec(timestampColumn, timestampFormat, null))//optional
      .buildBeam()
  }

  def getCurator(configuration: Map[String, String]): CuratorFramework = {
    if (curator == null) {
      this.synchronized({
        if (curator == null) {
          val curatorConnect = configuration.getOrElse("curator.connect",
            throw new AnalysisException(s"option curator.connect is required.")
          )
          val curatorRetryBaseSleepMs = configuration
            .getOrElse("curator.retry.base.sleep", "100").toInt
          val curatorRetryMaxSleepMs = configuration.getOrElse("curator.retry.max.sleep", "3000").toInt
          val curatorMaxRetries = configuration
            .getOrElse("curator.max.retries", "5").toInt
          curator = CuratorFrameworkFactory.newClient(curatorConnect,
            new BoundedExponentialBackoffRetry(curatorRetryBaseSleepMs, curatorRetryMaxSleepMs, curatorMaxRetries)
          )
          curator.start()
        }
      })
    }
    curator
  }

  override def finalize(): Unit = {
    super.finalize()
    if (curator != null) {
      try {
        curator.close()
      } catch {
        case e: Exception => logWarning("Exception when close zkClient.", e)
      }
    }
  }
}

case class SchemaInternalRow(schema: StructType, row: InternalRow, timestampColumn: String) {
  private val fieldNames = schema.fieldNames
  private val timeIndex = fieldNames.indexOf(timestampColumn)
  private val time = row.getLong(timeIndex)
  private val timeInMs = getTimeInMS(time)
  val ts = new DateTime(timeInMs)

  @JsonValue
  def toMap: collection.mutable.Map[String, Any] = {
    var rowMap = collection.mutable.Map[String, Any]("timestamp" -> timeInMs / 1000)
    for (ordinal <- fieldNames.indices) {
      val key =fieldNames(ordinal)

      if (!"timestamp".equals(key)) {
        val dataType = schema(ordinal).dataType
        dataType match {
          case _: BooleanType =>
            rowMap += (key -> row.getBoolean(ordinal))
          case _: ByteType =>
            rowMap += (key -> row.getByte(ordinal))
          case _: ShortType =>
            rowMap += (key -> row.getShort(ordinal))
          case _: IntegerType =>
            rowMap += (key ->row.getInt(ordinal))
          case _: LongType =>
            rowMap += (key ->row.getLong(ordinal))
          case _: FloatType =>
            rowMap += (key ->row.getFloat(ordinal))
          case _: DoubleType =>
            rowMap += (key ->row.getDouble(ordinal))
          case _: DateType =>
            rowMap += (key ->row.getInt(ordinal))
          case _: TimestampType =>
            rowMap += (key ->row.getLong(ordinal))
          case _: BinaryType =>
            rowMap += (key ->row.getBinary(ordinal))
          case _: StringType =>
            rowMap += (key ->row.getUTF8String(ordinal).toString)
          case _ =>
            throw new UnsupportedOperationException("Unsupported data type " + dataType.simpleString)
        }
      }
    }
    rowMap
  }

  def getTimeInSec(t: Long, row: InternalRow): Long = {
    if (timeInSec(t)) {
      t
    } else if (timeInMS(t)) {
      TimeUnit.MILLISECONDS.toSeconds(t)
    } else if (timeInNS(t)) {
      TimeUnit.NANOSECONDS.toSeconds(t)
    } else {
      throw new Exception(s"Invalid timestamp[${t}],timestamp should be second, millisecond or nanosecond.")
    }
  }

  def getTimeInMS(time: Long): Long = {
    if (timeInMS(time)) {
      time
    } else if (timeInSec(time)) {
      time * 1000
    } else if (timeInNS(time)) {
      TimeUnit.NANOSECONDS.toMicros(time)
    } else {
      throw new Exception(s"Invalid timestamp[${time}], timestamp should be second, millisecond or nanosecond.")
    }
  }

  def timeInSec(time: Long) = time.toString.length == 10
  def timeInMS(time: Long) = time.toString.length == 13
  def timeInNS(time: Long) = time.toString.length == 16
}
