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

import java.util.Locale

import org.apache.commons.cli.MissingArgumentException
import org.scalatest.FunSuite

class LoghubOffsetReaderSuite extends FunSuite {
  test("create loghub client used for one region") {
    val accessKeyId = "accessKeyId"
    val accessKeySecret = "accessKeySecret"
    val endpoint = "endpoint-cn-hangzhou"
    val sourceProps = Map(
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "endpoint" -> endpoint
    )
    LoghubOffsetReader.resetClientPool()
    LoghubOffsetReader.getOrCreateLoghubClient(sourceProps)
    assert(LoghubOffsetReader.logServiceClientPool.size == 1)

    LoghubOffsetReader.resetClientPool()
    LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
    assert(LoghubOffsetReader.logServiceClientPool.size == 1)
  }

  test("create loghub client used for more than one region") {
    LoghubOffsetReader.resetClientPool()
    Array("cn-hangzhou", "cn-beijing").foreach(region => {
      val accessKeyId = s"accessKeyId-$region"
      val accessKeySecret = s"accessKeySecret-$region"
      val endpoint = s"endpoint-$region"
      val sourceProps = Map(
        "access.key.id" -> accessKeyId,
        "access.key.secret" -> accessKeySecret,
        "endpoint" -> endpoint
      )
      LoghubOffsetReader.getOrCreateLoghubClient(sourceProps)
    })

    assert(LoghubOffsetReader.logServiceClientPool.size == 2)

    LoghubOffsetReader.resetClientPool()
    Array("cn-hangzhou", "cn-beijing").foreach(region => {
      val accessKeyId = s"accessKeyId-$region"
      val accessKeySecret = s"accessKeySecret-$region"
      val endpoint = s"endpoint-$region"
      LoghubOffsetReader.getOrCreateLoghubClient(accessKeyId, accessKeySecret, endpoint)
    })
    assert(LoghubOffsetReader.logServiceClientPool.size == 2)
  }

  test("bad options ") {
    def testMissingOptions(options: Map[String, String])(expectedMsgs: String*): Unit = {
      val ex = intercept[MissingArgumentException] {
        LoghubOffsetReader.getOrCreateLoghubClient(options)
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testMissingOptions(Map())("Missing access key id (='access.key.id').")

    testMissingOptions(Map(
      "access.key.id" -> "accessKeyId"
    ))("Missing access key secret (='access.key.secret').")

    testMissingOptions(Map(
      "access.key.id" -> "accessKeyId",
      "access.key.secret" -> "accessKeySecret"
    ))("Missing log store endpoint (='endpoint').")
  }
}
