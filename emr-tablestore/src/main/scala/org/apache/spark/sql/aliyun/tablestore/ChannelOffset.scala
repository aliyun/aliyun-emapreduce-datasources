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

/**
  * @param logPoint: channel checkpoint of Tablestore tunnel service.
  * @param offset: offset between under this channel checkpoint.
  */
case class ChannelOffset(logPoint: String, offset: Long)

object ChannelOffset {
  // BaseData channel or Father Stream channel would finally turn to Terminated.
  val TERMINATED_CHANNEL_OFFSET = ChannelOffset(TableStoreSourceProvider.OTS_CHANNEL_FINISHED, 0L)
}
