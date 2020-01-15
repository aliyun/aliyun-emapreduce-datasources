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
import java.text.SimpleDateFormat

import scala.collection.JavaConverters._

import com.aliyuncs.DefaultAcsClient
import com.aliyuncs.dts.model.v20180801.DescribeSubscriptionInstanceStatusRequest
import com.aliyuncs.profile.DefaultProfile
import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql.aliyun.dts.DTSSourceProvider._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getTimeZone
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.util.UninterruptibleThread

class DTSOffsetReader(options: DataSourceOptions) {

  private val df = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'")
  df.setTimeZone(getTimeZone("UTC"))

  private val instanceId = options.get(DTS_INSTANCE_ID).get()
  private val accessKeyId = options.get(ACCESS_KEY_ID).get
  private val accessKeySecret = options.get(ACCESS_KEY_SECRET).get
  private val endpoint = options.get(ENDPOINT).get
  private val topic = options.get(KAFKA_TOPIC).get
  private val tp = new TopicPartition(topic, 0)

  private val profile = DefaultProfile.getProfile(endpoint, accessKeyId, accessKeySecret)
  private var client = new DefaultAcsClient(profile)

  @volatile protected var _consumer: Consumer[Array[Byte], Array[Byte]] = null

  protected def consumer: Consumer[Array[Byte], Array[Byte]] = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    val consumerConfig = sourceKafkaProperties(options.asMap())
    if (_consumer == null) {
      _consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfig)
      _consumer.assign(Seq(tp).asJava)
    }
    _consumer
  }

  def fetchLatestOffsets(): PartitionOffset = {
    try {
//      val request = new DescribeSubscriptionInstanceStatusRequest()
//      request.setSubscriptionInstanceId(instanceId)
//      val response = client.getAcsResponse(request)
//      val endTimestamp = df.parse(response.getEndTimestamp).getTime / 1000
//      val remoteOffset = consumer.offsetsForTimes(ju.Collections.singletonMap(tp, endTimestamp))
//      val off = remoteOffset.get(tp)
      consumer.seekToEnd(Seq(tp).asJava)
      val off = consumer.position(tp)
      (tp, off)
    } catch {
      case e: Exception =>
        close()
        throw e
    }
  }

  def fetchEarliestOffsets(): PartitionOffset = {
    try {
//      val request = new DescribeSubscriptionInstanceStatusRequest()
//      request.setSubscriptionInstanceId(instanceId)
//      val response = client.getAcsResponse(request)
//      val startTimestamp = df.parse(response.getBeginTimestamp).getTime / 1000
//      val remoteOffset = consumer.offsetsForTimes(ju.Collections.singletonMap(tp, startTimestamp))
//      val off = remoteOffset.get(tp)
      consumer.seekToBeginning(Seq(tp).asJava)
      val off = consumer.position(tp)
      (tp, off)
    } catch {
      case e: Exception =>
        close()
        throw e
    }
  }

  def fetchSpecificOffsets(checkpoint: String): PartitionOffset = {
    try {
      val (timeStamp, _) = parseCheckpoint(checkpoint)
      val remoteOffset = consumer.offsetsForTimes(ju.Collections.singletonMap(tp, timeStamp))
      val off = remoteOffset.get(tp)
      consumer.seek(tp, off.offset())
      (tp, off.offset())
    } catch {
      case e: Exception =>
        close()
        throw e
    }
  }

  private def parseCheckpoint(checkpoint: String): (Long, Option[Long]) = {
    require(null != checkpoint, "checkpoint should not be null")
    val offsetAndTS: Array[String] = checkpoint.split("@")
    if (offsetAndTS.length == 1) {
      (offsetAndTS(0).toLong, None)
    } else {
      (offsetAndTS(0).toLong, Some(offsetAndTS(1).toLong))
    }
  }

  def close(): Unit = {
    client = null
    if (_consumer != null) {
      _consumer.close()
      _consumer = null
    }
  }
}
