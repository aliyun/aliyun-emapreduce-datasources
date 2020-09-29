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

class TableStoreRecordReaderSuite extends QueryTest with SharedSQLContext {
  private val testUtils = new TableStoreTestUtil()

  override def beforeEach(): Unit = {

    testUtils.deleteTable("t5")
    testUtils.deleteTable("t4")
    testUtils.deleteTable("t6")
    testUtils.createTable("t4")
    testUtils.createTable("t5")
    testUtils.createTable("t6")
    Thread.sleep(2000)
  }

  private def createDF(
    withOptions: Map[String, String] = Map.empty[String, String],
    tableName: String): DataFrame = {
    val options =
      testUtils.getTestOptions(
        Map(
          "catalog" -> TableStoreTestUtil.catalog,
          "maxOffsetsPerChannel" -> "10000"
        ),
        tableName,
        "",
        "true",
        "true"
      )
    val df = spark
      .read
      .format("tablestore")
    (withOptions ++ options).foreach {
      case (key, value) => df.option(key, value)
    }
    df.load()
  }

  test("test two table query") {
    val df1 = createDF(Map.empty, "t4")
    testUtils.insertData(50, "t4")
    val df2 = createDF(Map.empty, "t5")
    testUtils.insertData(50, "t5")
    val df3 = createDF(Map.empty, "t6")
    testUtils.insertData(50, "t6")
    assert(df1.select("PkString").count() == 50)
    assert(df3.select("PkString").count() == 50)
    assert(df1.select("PkString").count() == df2.select("PkString").count())
    assert(df1.select("PkString").count() == df3.select("PkString").count())
  }
}
