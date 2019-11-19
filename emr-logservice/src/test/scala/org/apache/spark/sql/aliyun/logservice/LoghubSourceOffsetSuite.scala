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

import com.aliyun.openservices.log.response.GetCursorResponse
import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.streaming.aliyun.logservice.LoghubClientAgent

class LoghubSourceOffsetSuite extends SparkFunSuite {

  private var testUtils: LoghubTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new LoghubTestUtils()
    testUtils.cleanAllResources()
    testUtils.init(Some(10))
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.cleanAllResources()
    }
  }

  class FakeOffset extends Offset {
    override def json(): String = "empty"
  }

  test("parsing loghub shard") {
    val json = """{"project#store":{"1":1000}}"""
    val loghubShard = LoghubShard("project", "store", 1)
    val result = Map(loghubShard -> 1000)

    try {
      val clientMock = mock(classOf[LoghubClientAgent])
      val cursorResponseMock = mock(classOf[GetCursorResponse])
      when(cursorResponseMock.GetCursor()).thenReturn("empty")
      when(clientMock.GetCursor("project", "store", 1, 1000L)).thenReturn(cursorResponseMock)
      LoghubOffsetReader.setLogServiceClient(testUtils.accessKeyId, testUtils.endpoint, clientMock)

      assert(LoghubSourceOffset.getShardOffsets(
        LoghubSourceOffset(result, testUtils.sourceProps), testUtils.sourceProps).keySet === Set(loghubShard))
      assert(LoghubSourceOffset.getShardOffsets(SerializedOffset(json), testUtils.sourceProps).keySet ===
        Set(loghubShard))
      try {
        LoghubSourceOffset.getShardOffsets(new FakeOffset, testUtils.sourceProps)
        assert(false, "Should throw a IllegalArgumentException.")
      } catch {
        case _: IllegalArgumentException =>
        // ok
        case _: Exception =>
          assert(false, "Should throw a IllegalArgumentException.")
      }
    } finally {
      LoghubOffsetReader.resetLogServiceClient(testUtils.accessKeyId, testUtils.endpoint)
    }
  }

  test("parsing partitionOffsets") {
    try {
      val clientMock = mock(classOf[LoghubClientAgent])
      val cursorResponseMock1 = mock(classOf[GetCursorResponse])
      when(cursorResponseMock1.GetCursor()).thenReturn("empty")
      when(clientMock.GetCursor("logProject-A", "logStore-B", 0, 1409569200L)).thenReturn(cursorResponseMock1)
      when(clientMock.GetCursor("logProject-A", "logStore-B", 1, 1409569201L)).thenReturn(cursorResponseMock1)
      val cursorResponseMock2 = mock(classOf[GetCursorResponse])
      when(cursorResponseMock2.GetCursor()).thenReturn("empty")
      when(clientMock.GetCursor("logProject-C", "logStore-D", 5, 1409569202L)).thenReturn(cursorResponseMock2)
      LoghubOffsetReader.setLogServiceClient(testUtils.accessKeyId, testUtils.endpoint, clientMock)

      val parsed = LoghubSourceOffset.partitionOffsets(
        """{"logProject-A#logStore-B":{"0":1409569200,"1":1409569201},"logProject-C#logStore-D":{"5":1409569202}}""",
        testUtils.sourceProps)
      assert(parsed(LoghubShard("logProject-A", "logStore-B", 0))._1 === 1409569200)
      assert(parsed(LoghubShard("logProject-A", "logStore-B", 1))._1 === 1409569201)
      assert(parsed(LoghubShard("logProject-C", "logStore-D", 5))._1 === 1409569202)
    } finally {
      LoghubOffsetReader.resetLogServiceClient(testUtils.accessKeyId, testUtils.endpoint)
    }
  }
}
