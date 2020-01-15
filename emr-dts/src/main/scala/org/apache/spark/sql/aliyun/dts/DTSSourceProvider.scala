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

import java.{util => ju}
import java.util.{Locale, Optional, Properties}

import scala.collection.JavaConverters._

import org.apache.commons.cli.MissingArgumentException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, MicroBatchReadSupport}
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.types._

class DTSSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with MicroBatchReadSupport {
  override def shortName(): String = "dts"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (shortName(), DTSSourceProvider.getSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    throw new UnsupportedOperationException("Do not support DTS data source v1")
  }

  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): MicroBatchReader = {
    DTSSourceProvider.checkOptions(options)

    val dtsOffsetReader = new DTSOffsetReader(options)
    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    val startingStreamOffsets = DTSSourceProvider.getDTSOffsetRangeLimit(caseInsensitiveParams,
      DTSSourceProvider.STARTING_OFFSETS_OPTION_KEY, LatestOffsetRangeLimit)
    new DTSMicroBatchReader(dtsOffsetReader, checkpointLocation, startingStreamOffsets, options)
  }
}

object DTSSourceProvider {
  val DTS_INSTANCE_ID = "instanceid"
  val SID_NAME = "sid"
  val ACCESS_KEY_ID = "accesskeyid"
  val ACCESS_KEY_SECRET = "accesskeysecret"
  val ENDPOINT = "endpoint"
  val USER_NAME = "user"
  val PASSWORD_NAME = "password"
  val KAFKA_BROKER_URL_NAME = "broker"
  val KAFKA_TOPIC = "kafkatopic"

  val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"

  def getSchema: StructType = {
    new StructType(Array(
      StructField("key", BinaryType),
      StructField("value", BinaryType),
      StructField("topic", StringType),
      StructField("partition", IntegerType),
      StructField("offset", LongType),
      StructField("timestamp", TimestampType),
      StructField("timestampType", IntegerType)
    ))
  }

  def getDTSOffsetRangeLimit(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: DTSOffsetRangeLimit): DTSOffsetRangeLimit = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffsetRangeLimit
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        EarliestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(json)
      case None => defaultOffsets
    }
  }

  def sourceKafkaProperties(originProperties: ju.Map[String, String]): Properties = {
    val sid = originProperties.get(SID_NAME).toString
    val user = originProperties.get(USER_NAME).toString
    val password = originProperties.get(PASSWORD_NAME).toString
    val kafkaBootstrapServers = originProperties.get(KAFKA_BROKER_URL_NAME).toString
    val consumerConfig = new Properties()
    originProperties.asScala.foreach { case (k, v) =>
      if (k.startsWith("kafka.")) {
        consumerConfig.put(k.substring(6), v)
      }
    }

    // scalastyle:off
    val jaasTemplate = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$user-$sid" password="$password";"""
    consumerConfig.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasTemplate)
    consumerConfig.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN")
    consumerConfig.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
    consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, sid)
    consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    // scalastyle:on

    consumerConfig
  }

  def checkOptions(options: DataSourceOptions): Unit = {
    if (!options.asMap().containsKey(DTS_INSTANCE_ID)) {
      throw new MissingArgumentException(s"Missing required argument '$DTS_INSTANCE_ID'.")
    }

    if (!options.asMap().containsKey(SID_NAME)) {
      throw new MissingArgumentException(s"Missing required argument '$SID_NAME'.")
    }

    if (!options.asMap().containsKey(ACCESS_KEY_ID)) {
      throw new MissingArgumentException(s"Missing required argument '$ACCESS_KEY_ID'.")
    }

    if (!options.asMap().containsKey(ACCESS_KEY_SECRET)) {
      throw new MissingArgumentException(s"Missing required argument '$ACCESS_KEY_SECRET'.")
    }

    if (!options.asMap().containsKey(ENDPOINT)) {
      throw new MissingArgumentException(s"Missing required argument '$ENDPOINT'.")
    }

    if (!options.asMap().containsKey(USER_NAME)) {
      throw new MissingArgumentException(s"Missing required argument '$USER_NAME'.")
    }

    if (!options.asMap().containsKey(PASSWORD_NAME)) {
      throw new MissingArgumentException(s"Missing required argument '$PASSWORD_NAME'.")
    }

    if (!options.asMap().containsKey(KAFKA_BROKER_URL_NAME)) {
      throw new MissingArgumentException(s"Missing required argument '$KAFKA_BROKER_URL_NAME'.")
    }

    if (!options.asMap().containsKey(KAFKA_TOPIC)) {
      throw new MissingArgumentException(s"Missing required argument '$KAFKA_TOPIC'.")
    }
  }
}
