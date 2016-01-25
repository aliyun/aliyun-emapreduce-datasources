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
package org.apache.spark.streaming.aliyun.logservice

import org.apache.spark.annotation.Experimental
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object LoghubUtils {
  /**
   *
   * @param ssc
   * @param mysqlHost
   * @param mysqlPort
   * @param mysqlDatabase
   * @param mysqlUser
   * @param mysqlPwd
   * @param mysqlWorkerInstanceTableName
   * @param mysqlShardLeaseTableName
   * @param logServiceProject
   * @param logStoreName
   * @param loghubConsumerGroupName
   * @param loghubInstanceNameBase
   * @param loghubEndpoint
   * @param accessKeyId
   * @param accessKeySecret
   * @param storageLevel
   * @return
   */
  @Experimental
  @deprecated("No need to provide \"loghubInstanceNameBase\" argument", "1.0.5")
  def createStream(
      ssc: StreamingContext,
      mysqlHost: String,
      mysqlPort: Int,
      mysqlDatabase: String,
      mysqlUser: String,
      mysqlPwd: String,
      mysqlWorkerInstanceTableName: String,
      mysqlShardLeaseTableName: String,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubInstanceNameBase: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      new LoghubInputDStream(
        ssc,
        mysqlHost,
        mysqlPort,
        mysqlDatabase,
        mysqlUser,
        mysqlPwd,
        mysqlWorkerInstanceTableName,
        mysqlShardLeaseTableName,
        logServiceProject,
        logStoreName,
        loghubConsumerGroupName,
        loghubInstanceNameBase,
        loghubEndpoint,
        accessKeyId,
        accessKeySecret,
        storageLevel)
    }
  }

  /**
   *
   * @param ssc
   * @param mysqlHost
   * @param mysqlPort
   * @param mysqlDatabase
   * @param mysqlUser
   * @param mysqlPwd
   * @param mysqlWorkerInstanceTableName
   * @param mysqlShardLeaseTableName
   * @param logServiceProject
   * @param logStoreName
   * @param loghubConsumerGroupName
   * @param loghubInstanceNameBase
   * @param loghubEndpoint
   * @param numReceivers
   * @param accessKeyId
   * @param accessKeySecret
   * @param storageLevel
   * @return
   */
  @Experimental
  @deprecated("No need to provide \"loghubInstanceNameBase\" argument", "1.0.5")
  def createStream(
      ssc: StreamingContext,
      mysqlHost: String,
      mysqlPort: Int,
      mysqlDatabase: String,
      mysqlUser: String,
      mysqlPwd: String,
      mysqlWorkerInstanceTableName: String,
      mysqlShardLeaseTableName: String,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubInstanceNameBase: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      ssc.union(Array.tabulate(numReceivers)(e => e).map(t =>
        new LoghubInputDStream(
          ssc,
          mysqlHost,
          mysqlPort,
          mysqlDatabase,
          mysqlUser,
          mysqlPwd,
          mysqlWorkerInstanceTableName,
          mysqlShardLeaseTableName,
          logServiceProject,
          logStoreName,
          loghubConsumerGroupName,
          loghubInstanceNameBase,
          loghubEndpoint,
          accessKeyId,
          accessKeySecret,
          storageLevel)
      ))
    }
  }

  /**
   *
   * @param ssc
   * @param mysqlHost
   * @param mysqlPort
   * @param mysqlDatabase
   * @param mysqlUser
   * @param mysqlPwd
   * @param mysqlWorkerInstanceTableName
   * @param mysqlShardLeaseTableName
   * @param logServiceProject
   * @param logStoreName
   * @param loghubConsumerGroupName
   * @param loghubEndpoint
   * @param accessKeyId
   * @param accessKeySecret
   * @param storageLevel
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      mysqlHost: String,
      mysqlPort: Int,
      mysqlDatabase: String,
      mysqlUser: String,
      mysqlPwd: String,
      mysqlWorkerInstanceTableName: String,
      mysqlShardLeaseTableName: String,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      new LoghubInputDStream(
        ssc,
        mysqlHost,
        mysqlPort,
        mysqlDatabase,
        mysqlUser,
        mysqlPwd,
        mysqlWorkerInstanceTableName,
        mysqlShardLeaseTableName,
        logServiceProject,
        logStoreName,
        loghubConsumerGroupName,
        appId,
        loghubEndpoint,
        accessKeyId,
        accessKeySecret,
        storageLevel)
    }
  }

  /**
   *
   * @param ssc
   * @param mysqlHost
   * @param mysqlPort
   * @param mysqlDatabase
   * @param mysqlUser
   * @param mysqlPwd
   * @param logServiceProject
   * @param logStoreName
   * @param loghubConsumerGroupName
   * @param loghubEndpoint
   * @param accessKeyId
   * @param accessKeySecret
   * @param storageLevel
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      mysqlHost: String,
      mysqlPort: Int,
      mysqlDatabase: String,
      mysqlUser: String,
      mysqlPwd: String,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): ReceiverInputDStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      new LoghubInputDStream(
        ssc,
        mysqlHost,
        mysqlPort,
        mysqlDatabase,
        mysqlUser,
        mysqlPwd,
        "loghub_worker",
        "loghub_lease",
        logServiceProject,
        logStoreName,
        loghubConsumerGroupName,
        appId,
        loghubEndpoint,
        accessKeyId,
        accessKeySecret,
        storageLevel)
    }
  }

  /**
   *
   * @param ssc
   * @param mysqlHost
   * @param mysqlPort
   * @param mysqlDatabase
   * @param mysqlUser
   * @param mysqlPwd
   * @param mysqlWorkerInstanceTableName
   * @param mysqlShardLeaseTableName
   * @param logServiceProject
   * @param logStoreName
   * @param loghubConsumerGroupName
   * @param loghubEndpoint
   * @param numReceivers
   * @param accessKeyId
   * @param accessKeySecret
   * @param storageLevel
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      mysqlHost: String,
      mysqlPort: Int,
      mysqlDatabase: String,
      mysqlUser: String,
      mysqlPwd: String,
      mysqlWorkerInstanceTableName: String,
      mysqlShardLeaseTableName: String,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      ssc.union(Array.tabulate(numReceivers)(e => e).map(t =>
        new LoghubInputDStream(
          ssc,
          mysqlHost,
          mysqlPort,
          mysqlDatabase,
          mysqlUser,
          mysqlPwd,
          mysqlWorkerInstanceTableName,
          mysqlShardLeaseTableName,
          logServiceProject,
          logStoreName,
          loghubConsumerGroupName,
          appId,
          loghubEndpoint,
          accessKeyId,
          accessKeySecret,
          storageLevel)
      ))
    }
  }

  /**
   *
   * @param ssc
   * @param mysqlHost
   * @param mysqlPort
   * @param mysqlDatabase
   * @param mysqlUser
   * @param mysqlPwd
   * @param logServiceProject
   * @param logStoreName
   * @param loghubConsumerGroupName
   * @param loghubEndpoint
   * @param numReceivers
   * @param accessKeyId
   * @param accessKeySecret
   * @param storageLevel
   * @return
   */
  @Experimental
  def createStream(
      ssc: StreamingContext,
      mysqlHost: String,
      mysqlPort: Int,
      mysqlDatabase: String,
      mysqlUser: String,
      mysqlPwd: String,
      logServiceProject: String,
      logStoreName: String,
      loghubConsumerGroupName: String,
      loghubEndpoint: String,
      numReceivers: Int,
      accessKeyId: String,
      accessKeySecret: String,
      storageLevel: StorageLevel): DStream[Array[Byte]] = {
    ssc.withNamedScope("loghub stream") {
      // Implicitly, we use applicationId to be the base name of loghub instance.
      val appId = ssc.sc.applicationId
      ssc.union(Array.tabulate(numReceivers)(e => e).map(t =>
        new LoghubInputDStream(
          ssc,
          mysqlHost,
          mysqlPort,
          mysqlDatabase,
          mysqlUser,
          mysqlPwd,
          "loghub_worker",
          "loghub_lease",
          logServiceProject,
          logStoreName,
          loghubConsumerGroupName,
          appId,
          loghubEndpoint,
          accessKeyId,
          accessKeySecret,
          storageLevel)
      ))
    }
  }
}
