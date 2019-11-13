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

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

case class TableStoreSourceOffset(uuid: String) extends Offset {
  override val json: String = uuid
}

private object TableStoreSourceOffset {
  def getUUID(offset: Offset): String = {
    offset match {
      case o: TableStoreSourceOffset => o.uuid
      case so: SerializedOffset => TableStoreSourceOffset(so.json).uuid
      case _ => throw new IllegalArgumentException(
        s"Invalid conversion from offset of ${offset.getClass} to TableStoreSourceOffset"
      )
    }
  }

  def apply(serializedOffset: SerializedOffset): TableStoreSourceOffset =
    TableStoreSourceOffset(serializedOffset.json)
}