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

package org.apache.spark.sql.aliyun.logservice

import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.aliyun.openservices.aliyun.log.producer.{LogProducer, Result}
import com.aliyun.openservices.log.Client
import com.aliyun.openservices.log.common.Consts.CursorMode
import com.aliyun.openservices.log.common._
import com.aliyun.openservices.log.exception.LogException
import com.google.common.util.concurrent.ListenableFuture

class LoghubTestUtils() {
  var accessKeyId: String = null
  var accessKeySecret: String = null
  var endpoint: String = null
  var client: Client = null
  var producer: LogProducer = null

  var logProject = "emr-sdk-ut-project"
  val logStorePool = new mutable.HashMap[String, Boolean]()

  var sourceProps: Map[String, String] = null

  val ttl = 1
  val numShards = 2
  val hashKeys = Map(
    0 -> "00000000000000000000000000000000",
    1 -> "fffffffffffffffffffffffffffffff0",
    2 -> "0e000000000000000000000000000000",
    3 -> "0h000000000000000000000000000000"
  )

  val lock = new Object

  def validateProps(): Unit = {
    accessKeyId = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
    accessKeySecret = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")

    val envType = Option(System.getenv("TEST_ENV_TYPE")).getOrElse("public").toLowerCase
    if (envType != "private" && envType != "public") {
      throw new Exception(s"Unsupported test environment type: $envType, only support private or public")
    }

    endpoint = envType match {
      case "private" => "cn-hangzhou-intranet.log.aliyuncs.com"
      case "public" => "cn-hangzhou.log.aliyuncs.com"
    }

    client = new Client(endpoint, accessKeyId, accessKeySecret)
    logProject = Option(System.getenv("LOGSTORE_PROJECT_NAME")).getOrElse("")
    sourceProps = Map(
      "sls.project" -> logProject,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "endpoint" -> endpoint
    )
    producer = LoghubOffsetReader.getOrCreateLogProducer(sourceProps)
  }

  validateProps()

  def newLogProject(): String = s"emr-ut-project-${UUID.randomUUID().toString}"

  def newLogStore(): String = s"emr-ut-store-${UUID.randomUUID().toString}"

  def init(timeWaitOption: Option[Int] = None): Unit = {
    val startTime = System.currentTimeMillis()
    Array.tabulate(20)(_ => {
      createLogStore(newLogStore())
    })
    val endTime = System.currentTimeMillis()
    val timeCost = endTime - startTime
    val timeWaite = timeWaitOption.getOrElse(60000)
    if (timeCost < timeWaite) {
      Thread.sleep(timeWaite - timeCost)
    }

    getAllLogStoreAndShardSize().foreach(e => createStoreIndex(e._1))
  }

  def createLogProject(projectName: String): Unit = {
    client.CreateProject(projectName, "test")
  }

  def createLogStore(storeName: String): Unit = {
    val logStore = new LogStore(storeName, ttl, numShards)
    client.CreateLogStore(logProject, logStore)
    logStorePool.put(storeName, false)
  }

  def createStoreIndex(storeName: String): Unit = {
    val indexLine = new IndexLine(new util.ArrayList[String](), false)
    val indexKeys = new IndexKeys()
    val tokens = new util.ArrayList[String]()
    tokens.add(",")
    tokens.add(".")
    tokens.add("#")
    val keyContent = new IndexKey(tokens, false, "text")
    indexKeys.AddKey("msg", keyContent)
    val index = new Index(1, indexKeys, indexLine)
    client.CreateIndex(logProject, storeName, index)
  }

  def getOneLogStore(): String = {
    lock.synchronized {
      val candidates = logStorePool.filter { case (_, used) => !used }
      if (candidates.nonEmpty) {
        val storeName = candidates.head._1
        logStorePool.put(storeName, true)
        storeName
      } else {
        init()
        getOneLogStore()
      }
    }
  }

  def sendMessages(msgs: List[String], storeName: String, shardId: Option[Int] = None): Unit = {
    var lastSendFuture: ListenableFuture[Result] = null
    msgs.foreach(msg => {
      val logItem = new LogItem((System.currentTimeMillis() / 1000).toInt)
      logItem.PushBack("msg", msg)
      shardId match {
        case Some(id) =>
          lastSendFuture = producer.send(logProject, storeName, "", "", hashKeys(id), logItem, null)
        case None =>
          lastSendFuture = producer.send(logProject, storeName, logItem)
      }
    })
    lastSendFuture.get()
  }

  def getLatestOffsets(logStore: String): Map[LoghubShard, (Int, String)] = {
    val shards = client.ListShard(logProject, logStore).GetShards().asScala
      .map(shard => LoghubShard(logProject, logStore, shard.GetShardId())).toSet
    shards.map {case loghubShard =>
        val cursor = client.GetCursor(logProject, logStore, loghubShard.shard, CursorMode.END)
        val cursorTime = client.GetCursorTime(logProject, logStore, loghubShard.shard, cursor.GetCursor())
        (loghubShard, (cursorTime.GetCursorTime(), cursor.GetCursor()))
      }.toMap
  }

  def getAllLogStoreAndShardSize(): Map[String, Int] = {
    val allLogStores = client.ListLogStores(logProject, 0, 500, "emr-ut-store").GetLogStores().asScala
    allLogStores.map(logStore => {
      val shards = client.ListShard(logProject, logStore).GetShards().asScala
      (logStore, shards.size)
    }).toMap
  }

  def splitLoghubShards(logStore: String): Unit = {
    client.SplitShard(logProject, logStore, 0, "0f000000000000000000000000000000")
  }

  def deleteLogStore(storeName: String): Unit = {
    client.DeleteLogStore(logProject, storeName)
  }

  def cleanAllResources(): Unit = {
    val allLogStores = client.ListLogStores(logProject, 0, 500, "emr-ut-store").GetLogStores().asScala
    allLogStores.foreach(logStore => {
      try {
        client.DeleteLogStore(logProject, logStore)
      } catch {
        case e: LogException if e.getMessage.contains(s"logstore $logStore does not exist") =>
          // ok
        case e: Exception => throw e
      }
    })
  }
}
