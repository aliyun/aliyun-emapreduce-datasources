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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.aliyun.dts.DTSSourceProvider._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class DTSRelationSuite extends QueryTest with SharedSQLContext {
  private var testUtils: DTSTestUtils = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new DTSTestUtils()
  }

  private def createDF(
      schema: Option[StructType],
      columnName: String = "value",
      withOptions: Map[String, String] = Map.empty[String, String]) = {
    val df = spark
      .read
      .format("dts")
      .option(SID_NAME, testUtils.sid)
      .option(PASSWORD_NAME, testUtils.password)
      .option(USER_NAME, testUtils.userName)
      .option(KAFKA_BROKER_URL_NAME, testUtils.bootstrapServer)
      .option(KAFKA_TOPIC, testUtils.topic)
    if (schema.isDefined) {
      df.schema(schema.get)
    }
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().select(columnName)
  }

  test("explicit earliest to latest offsets") {
    val df = createDF(None,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    assert(df.limit(10).count() == 10)
  }

  test("default starting and ending offsets") {
    val df = createDF(None)
    assert(df.limit(10).count() == 10)
  }

  test("reuse same dataframe in query") {
    val df = createDF(None,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    assert(df.union(df).limit(10).count() == 10)
  }
}
