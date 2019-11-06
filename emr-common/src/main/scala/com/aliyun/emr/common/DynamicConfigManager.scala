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

package com.aliyun.emr.common

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.retry.BoundedExponentialBackoffRetry

class DynamicConfigManager(rootPath: String, zkConnect: String, sessionTimeoutMs: Int) {
  private val curator: CuratorFramework = CuratorFrameworkFactory.newClient(
    zkConnect, new BoundedExponentialBackoffRetry(100, 100, 20))
  curator.start()

  private val zNodeChangeHandlers = new ConcurrentHashMap[(String, String), ZNodeChangeHandler]().asScala

  def registerZNodeChangeHandler(name: String, zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put((zNodeChangeHandler.path, name), zNodeChangeHandler)
  }

  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.keys.filter(k => k._1.equals(path))
      .foreach(zNodeChangeHandlers.remove)
  }

  private val configPathCache = new PathChildrenCache(curator, rootPath, true)

  private val configPathCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      val childPath = event.getData.getPath
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          zNodeChangeHandlers.filter(k => k._1._1.equals(childPath))
            .foreach(k => k._2.handleCreation())
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          zNodeChangeHandlers.filter(k => k._1._1.equals(childPath))
            .foreach(k => k._2.handleDeletion())
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          zNodeChangeHandlers.filter(k => k._1._1.equals(childPath))
            .foreach(k => k._2.handleDataChange())
        case PathChildrenCacheEvent.Type.CONNECTION_LOST =>
          curator.start()
        case PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED =>
          // todo
        case _ =>
        // do nothing
      }
    }
  }

  def start(): Unit = {
    configPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    configPathCache.getListenable.addListener(configPathCacheListener)
  }

  def stop(): Unit = {
    Try(configPathCache.getListenable.removeListener(configPathCacheListener))
    Try(configPathCache.close())
  }
}

object DynamicConfigManager {
  private val lock = new Object
  private var dynamicConfigManager: DynamicConfigManager = null

  def getOrCreateDynamicConfigManager(
      checkpointRoot: String,
      zkConnect: String,
      sessionTimeoutMs: Int = 10000): DynamicConfigManager = {
    lock.synchronized {
      if (dynamicConfigManager == null) {
        dynamicConfigManager = new DynamicConfigManager(checkpointRoot, zkConnect, sessionTimeoutMs)
        dynamicConfigManager.start()
      }

      dynamicConfigManager
    }
  }

  def stop(): Unit = {
    lock.synchronized {
      if (dynamicConfigManager != null) {
        dynamicConfigManager.stop()
      }
    }
  }
}

trait ZNodeChangeHandler {
  val path: String
  def handleCreation(): Unit = {}
  def handleDeletion(): Unit = {}
  def handleDataChange(): Unit = {}
}

trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange(): Unit = {}
}
