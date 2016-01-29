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

/**
 * Various utility classes for working with Aliyun LogService.
 *
 * Prepare Work:
 * {{{
 *   Before receiving log data, we need to create two mysql tables used by loghub:
 *
 *   Create Table if not exists loghub_lease
 *   (
 *     `auto_id` int(10) not null auto_increment,
 *     `consume_group` varchar(64),
 *     `logstream_sig` varchar(64),
 *     `shard_id` varchar(64),
 *     `lease_id` int(20),
 *     `lease_owner` varchar(64),
 *     `consumer_owner` varchar(64),
 *     `update_time` datetime,
 *     `checkpoint` text,
 *     PRIMARY KEY(`auto_id`),
 *     UNIQUE KEY(`consume_group`,`logstream_sig`, `shard_id`)
 *   )ENGINE = InnoDB DEFAULT CHARSET=utf8;

 *   Create Table if not exists loghub_worker
 *   (
 *     `auto_id` int(10) not null auto_increment,
 *     `consume_group` varchar(64),
 *     `logstream_sig` varchar(64),
 *     `instance_name` varchar(64),
 *     `update_time` datetime,
 *     PRIMARY KEY(`auto_id`),
 *     UNIQUE KEY(`consume_group`, `logstream_sig`, `instance_name`)
 *     )ENGINE = InnoDB DEFAULT CHARSET=utf8;
 * }}}
 */
object LoghubUtils {
  /**
   * Create loghub [[DStream]].
   *{{{
   *   val dbHost = "localhost"
   *   val dbPort = "3306"
   *   val db = "sample-db"
   *   val dbUser = "tom"
   *   var pwd = "123456"
   *   val instanceWorker = "loghub_worker"
   *   val lease = "loghub_lease"
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val instanceNameBase = "sample-namebase"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     dbHost,
   *     dbPort,
   *     db,
   *     dbUser,
   *     pwd,
   *     instanceWorker,
   *     lease,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     instanceNameBase,
   *     endpoint,
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param mysqlHost The host of Mysql needed by loghub.
   * @param mysqlPort  The port of Mysql needed by loghub.
   * @param mysqlDatabase The name of mysql database.
   * @param mysqlUser The username of mysql.
   * @param mysqlPwd The password of mysql.
   * @param mysqlWorkerInstanceTableName The name of `WorkInstance` table, such as "loghub_worker" above.
   * @param mysqlShardLeaseTableName The name of `ShardLease` table, such as "loghub_lease" above.
   * @param logServiceProject The name of `LogService` project
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubInstanceNameBase The name base of each loghub instance.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun AccessKeyId.
   * @param accessKeySecret The Aliyun AccessKeySecret.
   * @param storageLevel The storage level.
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
   * Create loghub [[DStream]].
   *{{{
   *   val dbHost = "localhost"
   *   val dbPort = "3306"
   *   val db = "sample-db"
   *   val dbUser = "tom"
   *   var pwd = "123456"
   *   val instanceWorker = "loghub_worker"
   *   val lease = "loghub_lease"
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val instanceNameBase = "sample-namebase"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val numReceivers = 2
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     dbHost,
   *     dbPort,
   *     db,
   *     dbUser,
   *     pwd,
   *     instanceWorker,
   *     lease,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     instanceNameBase,
   *     endpoint,
   *     numReceivers
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param mysqlHost The host of Mysql needed by loghub.
   * @param mysqlPort  The port of Mysql needed by loghub.
   * @param mysqlDatabase The name of mysql database.
   * @param mysqlUser The username of mysql.
   * @param mysqlPwd The password of mysql.
   * @param mysqlWorkerInstanceTableName The name of `WorkInstance` table, such as "loghub_worker" above.
   * @param mysqlShardLeaseTableName The name of `ShardLease` table, such as "loghub_lease" above.
   * @param logServiceProject The name of `LogService` project
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubInstanceNameBase The name base of each loghub instance.
   * @param loghubEndpoint The endpoint of loghub
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun AccessKeyId.
   * @param accessKeySecret The Aliyun AccessKeySecret.
   * @param storageLevel The storage level.
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
   *{{{
   *   val dbHost = "localhost"
   *   val dbPort = "3306"
   *   val db = "sample-db"
   *   val dbUser = "tom"
   *   var pwd = "123456"
   *   val instanceWorker = "loghub_worker"
   *   val lease = "loghub_lease"
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     dbHost,
   *     dbPort,
   *     db,
   *     dbUser,
   *     pwd,
   *     instanceWorker,
   *     lease,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     endpoint,
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param mysqlHost The host of Mysql needed by loghub.
   * @param mysqlPort  The port of Mysql needed by loghub.
   * @param mysqlDatabase The name of mysql database.
   * @param mysqlUser The username of mysql.
   * @param mysqlPwd The password of mysql.
   * @param mysqlWorkerInstanceTableName The name of `WorkInstance` table, such as "loghub_worker" above.
   * @param mysqlShardLeaseTableName The name of `ShardLease` table, such as "loghub_lease" above.
   * @param logServiceProject The name of `LogService` project
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun AccessKeyId.
   * @param accessKeySecret The Aliyun AccessKeySecret.
   * @param storageLevel The storage level.
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
   * Create loghub [[DStream]].
   *{{{
   *   val dbHost = "localhost"
   *   val dbPort = "3306"
   *   val db = "sample-db"
   *   val dbUser = "tom"
   *   var pwd = "123456"
   *   val instanceWorker = "loghub_worker"
   *   val lease = "loghub_lease"
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val numReceivers = 2
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     dbHost,
   *     dbPort,
   *     db,
   *     dbUser,
   *     pwd,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     endpoint,
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param mysqlHost The host of Mysql needed by loghub.
   * @param mysqlPort  The port of Mysql needed by loghub.
   * @param mysqlDatabase The name of mysql database.
   * @param mysqlUser The username of mysql.
   * @param mysqlPwd The password of mysql.
   * @param logServiceProject The name of `LogService` project
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param accessKeyId The Aliyun AccessKeyId.
   * @param accessKeySecret The Aliyun AccessKeySecret.
   * @param storageLevel The storage level.
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
   * Create loghub [[DStream]].
   *{{{
   *   val dbHost = "localhost"
   *   val dbPort = "3306"
   *   val db = "sample-db"
   *   val dbUser = "tom"
   *   var pwd = "123456"
   *   val instanceWorker = "loghub_worker"
   *   val lease = "loghub_lease"
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val numReceivers = 2
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     dbHost,
   *     dbPort,
   *     db,
   *     dbUser,
   *     pwd,
   *     instanceWorker,
   *     lease,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     endpoint,
   *     numReceivers
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param mysqlHost The host of Mysql needed by loghub.
   * @param mysqlPort  The port of Mysql needed by loghub.
   * @param mysqlDatabase The name of mysql database.
   * @param mysqlUser The username of mysql.
   * @param mysqlPwd The password of mysql.
   * @param mysqlWorkerInstanceTableName The name of `WorkInstance` table, such as "loghub_worker" above.
   * @param mysqlShardLeaseTableName The name of `ShardLease` table, such as "loghub_lease" above.
   * @param logServiceProject The name of `LogService` project
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun AccessKeyId.
   * @param accessKeySecret The Aliyun AccessKeySecret.
   * @param storageLevel The storage level.
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
   * Create loghub [[DStream]].
   *{{{
   *   val dbHost = "localhost"
   *   val dbPort = "3306"
   *   val db = "sample-db"
   *   val dbUser = "tom"
   *   var pwd = "123456"
   *   val loghubProject = "sample-project"
   *   val logStream = "sample-logstore"
   *   val loghubGroupName = "sample-group"
   *   val endpoint = "cn-hangzhou-intranet.sls.aliyuncs.com"
   *   val accessKeyId = "kj7aY*******UYx6"
   *   val accessKeySecret = "AiNMAlxz*************1PxaPaL8t"
   *   val numReceivers = 2
   *   val batchInterval = Milliseconds(5 * 1000)
   *
   *   val conf = new SparkConf().setAppName("Test Loghub")
   *   val ssc = new StreamingContext(conf, batchInterval)
   *   val loghubStream = LoghubUtils.createStream(
   *     ssc,
   *     dbHost,
   *     dbPort,
   *     db,
   *     dbUser,
   *     pwd,
   *     loghubProject,
   *     logStream,
   *     loghubGroupName,
   *     endpoint,
   *     numReceivers
   *     accessKeyId,
   *     accessKeySecret,
   *     StorageLevel.MEMORY_AND_DISK)
   *
   *}}}
   * @param ssc StreamingContext.
   * @param mysqlHost The host of Mysql needed by loghub.
   * @param mysqlPort  The port of Mysql needed by loghub.
   * @param mysqlDatabase The name of mysql database.
   * @param mysqlUser The username of mysql.
   * @param mysqlPwd The password of mysql.
   * @param logServiceProject The name of `LogService` project
   * @param logStoreName The name of logStore.
   * @param loghubConsumerGroupName The group name of loghub consumer. All consumer process which has the same group
   *                                name will consumer specific logStore together.
   * @param loghubEndpoint The endpoint of loghub.
   * @param numReceivers The number of receivers.
   * @param accessKeyId The Aliyun AccessKeyId.
   * @param accessKeySecret The Aliyun AccessKeySecret.
   * @param storageLevel The storage level.
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
