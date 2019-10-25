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
package org.apache.spark.streaming.aliyun.datahub

import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.model.GetCursorRequest.CursorType
import com.aliyun.datahub.model.RecordEntry
import com.aliyun.datahub.{DatahubClient, DatahubConfiguration}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.dstream.DStream

@deprecated("""Please use 'spark.readStream.format("datahub")' instead.""", "1.8.0")
object DatahubUtils {
  /**
   * Scala constructor to create a DStream from datahub source.
   * @param projectName datahub project name
   * @param topicName datahub topic name
   * @param subId datahub subscription id
   * @param accessKeyId aliyun access key id
   * @param accessKeySecret aliyun access key secret
   * @param endpoint datahub endpoint
   * @param shardId datahub topic shard
   * @param func handle RecordEntry to String
   */
  def createStream(
      ssc: StreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      shardId: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    ssc.withNamedScope("datahub stream") {
      new DatahubDStream(
        ssc,
        projectName,
        topicName,
        subId,
        accessKeyId,
        accessKeySecret,
        endpoint,
        shardId,
        func,
        storageLevel)
    }
  }

  def createStream(
      jssc: JavaStreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      shardId: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    createStream(jssc.ssc, projectName, topicName, subId, accessKeyId, accessKeySecret, endpoint, shardId, func,
      storageLevel)
  }

  /**
   * Scala constructor to create a DStream from datahub source.
   * @param projectName datahub project name
   * @param topicName datahub topic name
   * @param subId datahub subscription id
   * @param accessKeyId aliyun access key id
   * @param accessKeySecret aliyun access key secret
   * @param endpoint datahub endpoint
   * @param func handle RecordEntry to String
   */
  def createStream(
      ssc: StreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    val account = new AliyunAccount(accessKeyId, accessKeySecret)
    val conf = new DatahubConfiguration(account, endpoint)
    val loghubClient = new DatahubClient(conf)
    import scala.collection.JavaConverters._
    // no need to catch exception or retry if datahub-service error
    val shardEntries = loghubClient.listShard(projectName, topicName).getShards.asScala
    var dStream: DStream[Array[Byte]] = null

    for (shardEntry <- shardEntries) {
      if (dStream == null) {
        dStream = createStream(ssc, projectName, topicName, subId, accessKeyId, accessKeySecret, endpoint,
          shardEntry.getShardId, func, storageLevel)
      } else {
        dStream = dStream.union(createStream(ssc, projectName, topicName, subId, accessKeyId, accessKeySecret, endpoint,
          shardEntry.getShardId, func, storageLevel))
      }
    }

    dStream
  }

  def createStream(
      jssc: JavaStreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    createStream(jssc.ssc, projectName, topicName, subId, accessKeyId, accessKeySecret, endpoint, func, storageLevel)
  }

  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each datahub shard correspond to a RDD partition.
   * The Spark configuration spark.streaming.datahub.maxRatePerShard sets
   * max number of records per shard every second, 1000 by default.
   * @param project datahub project name
   * @param topic datahub topic name
   * @param subId datahub subscription id
   * @param accessId aliyun access key id
   * @param accessKey aliyun access key secret
   * @param endpoint datahub endpoint
   * @param func handle RecordEntry to String
   * @param mode datahub cursor type
   * @param zkParam Zookeeper configuration
   */
  def createDirectStream(
      ssc: StreamingContext,
      endpoint: String,
      project: String,
      topic: String,
      subId: String,
      accessId: String,
      accessKey: String,
      func: RecordEntry => String,
      mode: CursorType,
      zkParam: Map[String, String]): DStream[Array[Byte]] = {
    ssc.withNamedScope("datahub direct stream") {
      new DirectDatahubInputDStream(
        ssc,
        endpoint,
        project,
        topic,
        subId,
        accessId,
        accessKey,
        func,
        mode,
        zkParam)
    }
  }

  def createDirectStream(
      jssc: JavaStreamingContext,
      endpoint: String,
      project: String,
      topic: String,
      subId: String,
      accessId: String,
      accessKey: String,
      func: RecordEntry => String,
      mode: CursorType,
      zkParam: Map[String, String]): DStream[Array[Byte]] = {
    createDirectStream(jssc.ssc, endpoint, project, topic, subId, accessId, accessKey, func, mode, zkParam)
  }

  /**
   * :: Experimental ::
   * Scala constructor for a DStream where
   * each datahub shard correspond to a RDD partition.
   * The Spark configuration spark.streaming.datahub.maxRatePerShard sets
   * max number of records per shard every second.
   * @param project datahub project name
   * @param topic datahub topic name
   * @param subId datahub subscription id
   * @param accessId aliyun access key id
   * @param accessKey aliyun access key secret
   * @param endpoint datahub endpoint
   * @param func handle RecordEntry to String
   * @param zkParam Zookeeper configuration
   */
  def createDirectStream(
      ssc: StreamingContext,
      endpoint: String,
      project: String,
      topic: String,
      subId: String,
      accessId: String,
      accessKey: String,
      func: RecordEntry => String,
      zkParam: Map[String, String]): DStream[Array[Byte]] = {
    createDirectStream(ssc,
      endpoint,
      project,
      topic,
      subId,
      accessId,
      accessKey,
      func,
      CursorType.LATEST,
      zkParam)
  }

  def createDirectStream(
      jssc: JavaStreamingContext,
      endpoint: String,
      project: String,
      topic: String,
      subId: String,
      accessId: String,
      accessKey: String,
      func: RecordEntry => String,
      zkParam: Map[String, String]): DStream[Array[Byte]] = {
    createDirectStream(jssc.ssc,
      endpoint,
      project,
      topic,
      subId,
      accessId,
      accessKey,
      func,
      CursorType.LATEST,
      zkParam)
  }
}

/**
 * @deprecated As of release 1.8.0, please use spark.readStream.format("datahub") instead.
 */
@Deprecated
class DatahubUtilsHelper {
  /**
   * Java constructor to create a DStream from datahub source.
   * @param projectName datahub project name
   * @param topicName datahub topic name
   * @param subId datahub subscription id
   * @param accessKeyId aliyun access key id
   * @param accessKeySecret aliyun access key secret
   * @param endpoint datahub endpoint
   * @param shardId datahub topic shard
   * @param func handle RecordEntry to String
   */
  def createStream(
      jssc: JavaStreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      shardId: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    DatahubUtils.createStream(
      jssc.ssc,
      projectName,
      topicName,
      subId,
      accessKeyId,
      accessKeySecret,
      endpoint,
      shardId,
      func,
      storageLevel)
  }

  /**
   * Java constructor to create a DStream from datahub source.
   * @param projectName datahub project name
   * @param topicName datahub topic name
   * @param subId datahub subscription id
   * @param accessKeyId aliyun access key id
   * @param accessKeySecret aliyun access key secret
   * @param endpoint datahub endpoint
   * @param func handle RecordEntry to String
   */
  def createStream(
      jssc: JavaStreamingContext,
      projectName: String,
      topicName: String,
      subId: String,
      accessKeyId: String,
      accessKeySecret: String,
      endpoint: String,
      func: RecordEntry => String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    DatahubUtils.createStream(
      jssc.ssc,
      projectName,
      topicName,
      subId,
      accessKeyId,
      accessKeySecret,
      endpoint,
      func,
      storageLevel)
  }

  /**
   * :: Experimental ::
   * Java constructor for a DStream where
   * each datahub shard correspond to a RDD partition.
   * The Spark configuration spark.streaming.datahub.maxRatePerShard sets
   * max number of records per shard every second.
   * @param project datahub project name
   * @param topic datahub topic name
   * @param subId datahub subscription id
   * @param accessId aliyun access key id
   * @param accessKey aliyun access key secret
   * @param endpoint datahub endpoint
   * @param func handle RecordEntry to String
   * @param mode datahub cursor type
   * @param zkParam Zookeeper configuration
   */
  def createDirectStream(
      jssc: JavaStreamingContext,
      endpoint: String,
      project: String,
      topic: String,
      subId: String,
      accessId: String,
      accessKey: String,
      func: RecordEntry => String,
      mode: CursorType,
      zkParam: Map[String, String]): DStream[Array[Byte]] = {
    DatahubUtils.createDirectStream(jssc.ssc, endpoint, project, topic, subId, accessId, accessKey, func, mode, zkParam)
  }

  /**
   * :: Experimental ::
   * Java constructor for a DStream where
   * each datahub shard correspond to a RDD partition.
   * The Spark configuration spark.streaming.datahub.maxRatePerShard sets
   * max number of records per shard every second.
   * @param project datahub project name
   * @param topic datahub topic name
   * @param subId datahub subscription id
   * @param accessId aliyun access key id
   * @param accessKey aliyun access key secret
   * @param endpoint datahub endpoint
   * @param func handle RecordEntry to String
   * @param zkParam Zookeeper configuration
   */
  def createDirectStream(
      jssc: JavaStreamingContext,
      endpoint: String,
      project: String,
      topic: String,
      subId: String,
      accessId: String,
      accessKey: String,
      func: RecordEntry => String,
      zkParam: Map[String, String]): DStream[Array[Byte]] = {
    DatahubUtils.createDirectStream(jssc.ssc,
      endpoint,
      project,
      topic,
      subId,
      accessId,
      accessKey,
      func,
      CursorType.LATEST,
      zkParam)
  }
}
