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

import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession


class TableStoreSourceProviderSuite extends SparkFunSuite {
  private val testUtils = new TableStoreTestUtil()

  test("validate full data options") {
    val spark = SparkSession.builder
      .appName("TestValidateOptions")
      .master("local[1]")
      .getOrCreate()
    try {
      testUtils.createTestRelation(spark.sqlContext, Map("catalog" -> TableStoreTestUtil.catalog))
    } catch {
      // should not achieve here.
      case ex: Throwable => fail(ex)
    }
  }

  test("validate stream options without tunnelId") {
    val spark = SparkSession.builder
      .appName("TestValidateOptions")
      .master("local[1]")
      .getOrCreate()

    try {
      testUtils.createTestSource(spark.sqlContext, Map("catalog" -> TableStoreTestUtil.catalog))
    } catch {
      case ex: MissingArgumentException => succeed
      case ex: Throwable => fail(ex)
    }
  }

  test("validate stream options") {
    val spark = SparkSession.builder
      .appName("TestValidateOptions")
      .master("local[1]")
      .getOrCreate()

    try {
      testUtils.createTestSource(spark.sqlContext, Map(
        "catalog" -> TableStoreTestUtil.catalog, "tunnel.id" -> "testTunnelId"))
    } catch {
      case ex: Throwable => print(ex)
    }
  }

}
