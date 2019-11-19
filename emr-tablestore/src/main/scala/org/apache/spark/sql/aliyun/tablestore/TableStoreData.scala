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
package org.apache.spark.sql.aliyun.tablestore

import com.alibaba.fastjson.JSONObject

import org.apache.spark.internal.Logging
import org.apache.spark.sql.aliyun.tablestore.TableStoreSourceProvider._

abstract class TableStoreData(content: Array[(String, Any)]) extends Serializable {
  def getContent: Array[Byte]

  def toArray: Array[Any]
}

class SchemaTableStoreData(
    recordType: String,
    recordTimeStamp: Long,
    content: Array[(String, Any)])
  extends TableStoreData(content) with Logging with Serializable {
  override def getContent: Array[Byte] = {
    val obj = new JSONObject()
    obj.put(__OTS_RECORD_TYPE__, recordType)
    obj.put(__OTS_RECORD_TIMESTAMP__, recordTimeStamp)
    content.foreach(o => obj.put(o._1, o._2))
    obj.toJSONString.getBytes
  }

  override def toArray: Array[Any] = {
    Array(recordType, recordTimeStamp) ++: content.map(_._2)
  }
}
