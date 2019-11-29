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

import com.alicloud.openservices.tablestore.model.tunnel.{TunnelStage, TunnelType}

import org.apache.spark.sql.QueryTest
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
    tunnelId: String,
    withOptions: Map[String, String] = Map.empty[String, String]) = {
    val options =
      testUtils.getTestOptions(
        Map(
          "catalog" -> TableStoreTestUtil.catalog,
          "tunnel.id" -> tunnelId,
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
    val tunnelId = testUtils.createTunnel(TunnelType.BaseData)
    while (!testUtils.checkTunnelReady(tunnelId, TunnelStage.ProcessBaseData)) {
      Thread.sleep(2000)
    }
    testUtils.insertData(50000)

    val df = createDF(tunnelId, Map.empty)
    assert(df.select("PkString").count() == 50000)
    assert(df.select("*").count() == 50000)
  }
}
