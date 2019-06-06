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

package org.apache.spark.sql.druid

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
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType
import org.joda.time.{DateTime, Period}


object DruidWriter {
  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      parameters: Map[String, String]): Unit = {
    val schema = queryExecution.analyzed.schema
    import com.metamx.tranquility.spark.BeamRDD._
    queryExecution.toRdd.map(SchemaInternalRow(schema, _)).propagate(new EventBeamFactory(parameters))
  }
}


class EventBeamFactory(druidConfiguration: Map[String, String]) extends BeamFactory[SchemaInternalRow]
{
  def makeBeam: Beam[SchemaInternalRow] = EventBeamFactory.BeamInstance(druidConfiguration)
}

object EventBeamFactory extends Logging {
  private var curator: CuratorFramework = null
  private val mapper = new ObjectMapper()
  mapper.registerModule(new AggregatorsModule())

  def BeamInstance (druidConfiguration: Map[String, String]): Beam[SchemaInternalRow] = {
    val curatorConnect = druidConfiguration.getOrElse("curator.connect",
      throw new AnalysisException(s"option curator.connect is required.")
    )
    val indexService = druidConfiguration.getOrElse("index.service",
      throw new AnalysisException(s"option index.service is required.")
    )
    val dataSource = druidConfiguration.getOrElse("data.source",
      throw new AnalysisException(s"option data.source is required.")
    )
    val firehouse = druidConfiguration.getOrElse("firehouse",
      throw new AnalysisException(s"option firehouse is required.")
    )
    val metricsSpec = druidConfiguration.getOrElse("metricsSpec",
      throw new AnalysisException(s"option metricsSpec is required. format: " +
        s"""{\\"metricsSpec\\":[{\\"type\\":\\"count\\",\\"name\\":\\"count\\"},{\\"type\\":\\"doubleSum\\",\\"fieldName\\":\\"x\\",\\"name\\":\\"x\\"}]}""")
    )
    val rollupDimensions = druidConfiguration.getOrElse("rollup.dimensions",
      throw new AnalysisException(s"option rollup.dimensions is required, with format dimension1,dimension2....")
    )
    val rollupQueryGranularities = druidConfiguration.getOrElse("rollup.query.granularities",
      throw new AnalysisException(s"option rollup.query.granularities is required.")
    )
    val discoveryPath = druidConfiguration.getOrElse("discovery.path", "/druid/discovery")
    val curatorRetryBaseSleepMs = druidConfiguration
      .getOrElse("curator.retry.base.sleep", "100").toInt
    val curatorRetryMaxSleepMs = druidConfiguration.getOrElse("curator.retry.max.sleep", "3000").toInt
    val curatorMaxRetries = druidConfiguration
      .getOrElse("curator.max.retries", "5").toInt
    val tuningSegmentGranularity = druidConfiguration
      .getOrElse("curator.max.tuning.segment.granularity","DAY")
    val segmentGranularity = Granularity.valueOf(tuningSegmentGranularity.toUpperCase)
    val tuningWindowPeriod = druidConfiguration
      .getOrElse("curator.max.tuning.tuning.window.period", "PT10M")
    val tuningPartitions = druidConfiguration
      .getOrElse("curator.max.tuning.partitions", "1").toInt
    val tuningReplicants = druidConfiguration
      .getOrElse("curator.max.tuning.replications", "1").toInt
    val tuningWarmingPeriod = druidConfiguration
      .getOrElse("curator.max.tuning.warming.period", "0").toInt
    val warmingPeriod: Period = new Period(tuningWarmingPeriod)
    val timestampColumn = druidConfiguration.getOrElse("curator.max.tuning.column", "timestamp")
    val timestampFormat = druidConfiguration
      .getOrElse("curator.max.tuning.timestamp.format", "iso")

    if (curator == null) {
      curator = CuratorFrameworkFactory.newClient(curatorConnect,
        new BoundedExponentialBackoffRetry(curatorRetryBaseSleepMs, curatorRetryMaxSleepMs, curatorMaxRetries)
      )
      this.synchronized {
        if (!curator.isStarted) {
          curator.start()
        }
      }

    }

    val aggregators = mapper.readValue(metricsSpec, classOf[AggregatorFactories])

    val dimensions = rollupDimensions.split(",").map(_.trim())

    // TODO: CALENDRIC_GRANULARITIES
    val queryGranularities = rollupQueryGranularities.toLowerCase() match {
      case "all" => QueryGranularities.ALL
      case "none" => QueryGranularities.NONE
      case time =>  QueryGranularity.fromString(time)
    }

    // for test
    val location = DruidLocation(indexService, firehouse, dataSource)
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

case class SchemaInternalRow(schema: StructType, row: InternalRow) {
  private val schemaRow = schema.toList.map(_.name).zip(row.toSeq(schema))
  private val time = schemaRow.toMap.getOrElse("timestamp",
    throw new Exception("fail to find column named timestamp.")).asInstanceOf[Long]
  private val isTimeInSec = timeInSec(time)
  private var timeInMs = time
  if (isTimeInSec) {
    timeInMs = time * 1000
  } else if (timeInNS(time)) {
    timeInMs = TimeUnit.NANOSECONDS.toMicros(time)
  } else if (!timeInMS(time)) {
    throw new Exception(s"invalid timestamp[${time}], timestamp should be second, millisecond or nanosecond.")
  }
  val ts = new DateTime(timeInMs)

  @JsonValue
  def toMap: Map[String, Any] = {
    if (!isTimeInSec) {
      val ret = collection.mutable.Map(schemaRow:_*)
      ret("timestamp") = (timeInMs / 1000).toLong
      ret.toMap
    } else {
      schemaRow.toMap
    }
  }

  def timeInSec(time: Long) = time.toString.length == 10
  def timeInMS(time: Long) = time.toString.length == 13
  def timeInNS(time: Long) = time.toString.length == 16
}
