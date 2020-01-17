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
package org.apache.spark.sql.aliyun.dts

import java.util.Locale

import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.sql.aliyun.dts.DTSSourceProvider._
import org.apache.spark.sql.test.SharedSQLContext

class DTSMicroBatchSourceSuite extends SharedSQLContext {
  test("bad query options") {
    def testMissingOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[MissingArgumentException] {
        val reader = spark
          .readStream
          .format("dts")
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testMissingOptions()(s"Missing required argument '$SID_NAME'.")

    // Multiple strategies specified
    testMissingOptions(SID_NAME -> "sid")(s"Missing required argument '$USER_NAME'.")

    testMissingOptions(
      SID_NAME -> "sid",
      USER_NAME -> "user.name")(
      s"Missing required argument '$PASSWORD_NAME'.")

    testMissingOptions(
      SID_NAME -> "sid",
      USER_NAME -> "user.name",
      PASSWORD_NAME -> "password")(
      s"Missing required argument '$KAFKA_BROKER_URL_NAME'.")

    testMissingOptions(
      SID_NAME -> "sid",
      USER_NAME -> "user.name",
      PASSWORD_NAME -> "password",
      KAFKA_BROKER_URL_NAME -> "brokerUrl")(
      s"Missing required argument '$KAFKA_TOPIC'.")
  }
}
