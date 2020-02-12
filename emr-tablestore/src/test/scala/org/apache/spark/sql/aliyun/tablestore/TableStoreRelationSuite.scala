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

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSQLContext

class TableStoreRelationSuite extends QueryTest with SharedSQLContext {
  private val testUtils = new TableStoreTestUtil()

  override def beforeEach(): Unit = {
    testUtils.deleteTunnel()
    testUtils.deleteTable()
    testUtils.createTable()
    Thread.sleep(2000)
  }

  private def createDF(
    withOptions: Map[String, String] = Map.empty[String, String]): DataFrame = {
    val options =
      testUtils.getTestOptions(
        Map(
          "catalog" -> TableStoreTestUtil.catalog,
          "maxOffsetsPerChannel" -> "10000"
        )
      )
    val df = spark
      .read
      .format("tablestore")
    (withOptions ++ options).foreach {
      case (key, value) => df.option(key, value)
    }
    df.load()
  }

  test("select * or column from tablestore relation") {
    testUtils.insertData(50000)

    val df = createDF(Map.empty)
    assert(df.select("PkString").count() == 50000)
    assert(df.select("col5").count() == 50000)
    assert(df.select("*").count() == 50000)
    assert(df.select("col1", "col6", "PkInt", "PkString").count() == 50000)
  }

  test("select * from tablestore with single filter") {
    testUtils.insertData(50000)

    val df = createDF(Map.empty)
    assert(df.select("*").filter("PkInt >= 10000").count() == 40000)
    assert(df.select("*").filter("PkInt > 10000").count() == 39999)
    assert(df.select("*").filter("PkInt < 10000").count() == 10000)
    assert(df.select("*").filter("PkInt <= 10000").count() == 10001)
    assert(df.select("*").filter("PkInt == 10000").count() == 1)
    assert(df.select("*").filter("PkInt != 10000").count() == 49999)
  }

  test("select columns from tablestore with single filter") {
    testUtils.insertData(50000)

    val df = createDF(Map.empty)
    assert(df.select("PkString", "col5").filter("PkInt >= 10000").count() == 40000)
    assert(df.select("PkString", "col1").filter("PkInt > 10000").count() == 39999)
    assert(df.select("col1", "col2").filter("PkInt < 10000").count() == 10000)
    assert(df.select("timestamp").filter("PkInt <= 10000").count() == 10001)
    assert(df.select("col3", "col5").filter("PkInt == 10000").count() == 1)
    assert(df.select("col1", "PkString", "PkInt", "col6").filter("PkInt != 10000").count() == 49999)
  }

  test("select columns from tablestore with complex filter") {
    testUtils.insertData(50000)

    val df = createDF(Map.empty)
    assert(df.select("PkString", "PkInt").filter(
      "PkInt >= 10000 AND col6 == true").count() == 20000)
    assert(df.select("PkString", "PkInt").filter(
      "(PkInt >= 10000 AND col6 == true) OR (col1 < 10000 AND col5 > 3)").count() == 30000)
    assert(df.select("col1", "col5", "PkString").filter(
      "PkString != '6666' AND PkInt != 8888").count() == 49998)
    assert(df.select("col1", "col5", "PkString").filter(
      "PkString == '6666' AND PkInt != 8888").count() == 1)
    assert(df.select("PkString", "PkInt").filter(
      "(PkString == 6666 OR (PkInt >= 10000 AND PkInt < 20000 AND col6 == true))").count() == 5001)
  }
}
