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

import com.alibaba.fastjson.JSONObject
import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._

abstract class LoghubData(
    val project: String, val store: String, val shardId: Int,
    val dataTime: java.sql.Timestamp, val topic: String, val source: String)
  extends Serializable {
  def getContent: Array[Byte]
  def toArray: Array[Any]
}

class SchemaLoghubData(project: String, store: String, shardId: Int, dataTime: java.sql.Timestamp,
                       topic: String, source: String, content: Array[(String, Any)])
  extends LoghubData(project, store, shardId, dataTime, topic, source)
    with Logging with Serializable {

  override def getContent: Array[Byte] = {
    val obj = new JSONObject()
    obj.put(__TOPIC__, topic)
    obj.put(__SOURCE__, source)
    content.asInstanceOf[Array[(String, Any)]].foreach(r => {
      obj.put(r._1, r._2)
    })
    obj.toJSONString.getBytes
  }

  override def toArray: Array[Any] = {
    Array(project, store, shardId, dataTime, topic, source) ++: content.map(_._2)
  }
}

class RawLoghubData(project: String, store: String, shardId: Int, dataTime: java.sql.Timestamp,
                    topic: String, source: String, content: Array[Byte])
  extends LoghubData(project, store, shardId, dataTime, topic, source) {

  override def getContent: Array[Byte] = content

  override def toArray: Array[Any] = throw new UnsupportedOperationException
}