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

import org.scalatest.FunSuite

class ChannelOffsetSuite extends FunSuite {
  test("channel offset serialize and deserialize") {
    val offset = ChannelOffset("testlogPoint", 0)
    val serialized = ChannelOffset.serialize(offset)
    println(serialized)
    val deserialized = ChannelOffset.deserialize(serialized)
    assert(deserialized.logPoint == "testlogPoint")
    assert(deserialized.offset == 0)
  }
}
