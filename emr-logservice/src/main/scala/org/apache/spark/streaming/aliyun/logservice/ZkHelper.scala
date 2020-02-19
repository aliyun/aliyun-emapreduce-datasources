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

// scalastyle:off

import java.nio.charset.StandardCharsets

import scala.collection.JavaConversions._

// scalastyle:on
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.I0Itec.zkclient.serialize.ZkSerializer

import org.apache.spark.internal.Logging


class ZkHelper(
   zkParams: Map[String, String],
   checkpointDir: String,
   project: String,
   logstore: String) extends Logging {

  private val zkDir = s"$checkpointDir/commit/$project/$logstore"
  private val legacyCommitDir = s"$checkpointDir/consume/$project/$logstore"
  @transient private var zkClient: ZkClient = _

  def initialize(): Unit = {
    val zkConnect = zkParams.getOrElse("zookeeper.connect", "localhost:2181")
    val zkSessionTimeoutMs = zkParams.getOrElse("zookeeper.session.timeout.ms", "6000").toInt
    val zkConnectionTimeoutMs =
      zkParams.getOrElse("zookeeper.connection.timeout.ms", zkSessionTimeoutMs.toString).toInt
    logInfo(s"zkDir = $zkDir")

    zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
    zkClient.setZkSerializer(new ZkSerializer() {
      override def serialize(data: scala.Any): Array[Byte] = {
        data.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      }

      override def deserialize(bytes: Array[Byte]): AnyRef = {
        if (bytes == null) {
          return null
        }
        new String(bytes, StandardCharsets.UTF_8)
      }
    })
  }

  def mkdir(): Unit = {
    try {
      // Check if zookeeper is usable. Direct loghub api depends on zookeeper.
      if (!zkClient.exists(zkDir)) {
        zkClient.createPersistent(zkDir, true)
        return
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Loghub direct api depends on zookeeper. Make sure that " +
          "zookeeper is on active service.", e)
    }
    try {
      zkClient.getChildren(zkDir).foreach(child => {
        zkClient.delete(s"$zkDir/$child")
      })
    } catch {
      case _: ZkNoNodeException =>
        logDebug("If this is the first time to run, it is fine to not find any commit data in " +
          "zookeeper.")
    }
  }

  def readOffset(shardId: Int): String = {
    zkClient.readData(s"$zkDir/$shardId.shard")
  }

  def saveOffset(shard: Int, cursor: String): Unit = {
    val cursorFile = s"$zkDir/$shard.shard"
    writeZkFile(cursorFile, cursor)
  }

  private def writeZkFile(filePath: String, text: String): Unit = {
    logInfo(s"Save $text to $filePath")
    if (!zkClient.exists(filePath)) {
      zkClient.createPersistent(filePath, true)
    }
    zkClient.writeData(filePath, text)
  }

  def saveLegacyOffset(shard: Int, cursor: String): Unit = {
    val cursorFile = s"$legacyCommitDir/$shard.shard"
    writeZkFile(cursorFile, cursor)
  }

  def tryLock(shard: Int): Boolean = {
    val lockFile = s"$zkDir/$shard.lock"
    try {
      zkClient.createPersistent(lockFile, false)
      true
    } catch {
      case _: ZkNodeExistsException =>
        logWarning(s"$shard already locked")
        false
    }
  }

  def unlock(shard: Int): Unit = {
    try {
      zkClient.delete(s"$zkDir/$shard.lock")
    } catch {
      case _: ZkNoNodeException =>
      // ignore
    }
  }

  def close(): Unit = {
    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }
  }
}
