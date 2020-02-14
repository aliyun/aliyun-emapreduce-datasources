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
import scala.collection.JavaConversions._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.spark.internal.Logging


class ZkHelper(
   zkClient: ZkClient,
   checkpointDir: String,
   project: String,
   logstore: String) extends Logging {

  private val zkDir = s"$checkpointDir/commit/$project/$logstore"

  def mkdir(): Unit = {
    try {
      // Check if zookeeper is usable. Direct loghub api depends on zookeeper.
      val exists = zkClient.exists(s"$checkpointDir")
      if (!exists) {
        zkClient.createPersistent(zkDir, true)
        return
      }
    } catch {
      case e: Exception =>
        throw new RuntimeException("Loghub direct api depends on zookeeper. Make sure that " +
          "zookeeper is on active service.", e)
    }
    // Clean commit data in zookeeper in case of restarting streaming job but lose checkpoint file
    // in `checkpointDir`
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

  def tryLock(shard: Int): Boolean = {
    if (zkClient.exists(s"$zkDir/$shard.lock")) {
      return false
    }
    zkClient.createPersistent(s"$zkDir/$shard.lock", true)
    true
  }

  def unlock(shard: Int): Unit = {
    try {
      zkClient.delete(s"$zkDir/$shard.lock")
    } catch {
      case _: ZkNoNodeException =>
        // ignore
    }
  }

  def saveOffset(shard: Int, cursor: String): Unit = {
    val nodePath = s"$zkDir/$shard.shard"
    if (zkClient.exists(nodePath)) {
      zkClient.writeData(nodePath, cursor)
    } else {
      zkClient.createPersistent(nodePath, true)
      zkClient.writeData(nodePath, cursor)
    }
  }

}
