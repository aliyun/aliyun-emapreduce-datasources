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
package org.apache.spark.sql.aliyun.dts

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class DTSRelation(
    override val sqlContext: SQLContext,
    sourceOptions: CaseInsensitiveMap[String],
    specifiedKafkaParams: Map[String, String],
    startingOffsets: DTSOffsetRangeLimit,
    endingOffsets: DTSOffsetRangeLimit) extends BaseRelation with TableScan with Logging {
  assert(startingOffsets != LatestOffsetRangeLimit,
    "Starting offset not allowed to be set to latest offsets.")
  assert(endingOffsets != EarliestOffsetRangeLimit,
    "Ending offset not allowed to be set to earliest offsets.")

  private val pollTimeoutMs = sourceOptions.getOrElse(
    "kafkaConsumer.pollTimeoutMs",
    (Utils.timeStringAsSeconds(
      SparkEnv.get.conf.get("spark.network.timeout", "120s")) * 1000L).toString
  ).toLong

  override def schema: StructType = DTSSourceProvider.getSchema

  override def buildScan(): RDD[Row] = {
    val offsetReader = new DTSOffsetReader(new DataSourceOptions(sourceOptions.asJava))

    val (fromPartitionOffsets, untilPartitionOffsets) = {
      try {
        (offsetReader.fetchPartitionOffsets(startingOffsets),
          offsetReader.fetchPartitionOffsets(endingOffsets))
      } finally {
        offsetReader.close()
      }
    }

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val executorKafkaParams = DTSSourceProvider.sourceKafkaProperties(sourceOptions.asJava)
    val rdd = new DTSSourceRDD(
      sqlContext.sparkContext,
      sourceOptions,
      executorKafkaParams,
      fromPartitionOffsets,
      untilPartitionOffsets,
      pollTimeoutMs)
    sqlContext.internalCreateDataFrame(rdd.setName("dts"), schema).rdd
  }

  override def toString: String =
    s"DTSRelation(start=$startingOffsets, end=$endingOffsets)"
}
