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

package org.apache.spark.sql.aliyun.udfs.tablestore

import org.apache.spark.SparkFunSuite

class ResolveTableStoreBinlogUDFSuite extends SparkFunSuite {
  test("resolve tablestore binlog udf") {
    var actualLong = ResolveTableStoreBinlogUDF.getActualValue[Long](10000L, "PUT")
    assert(actualLong == 10000L)
    actualLong = ResolveTableStoreBinlogUDF.getActualValue[Long](10000L, "DELETE_ONE_VERSION")
    assert(actualLong == 0)
    actualLong = ResolveTableStoreBinlogUDF.getActualValue[Long](10000L, "DELETE_ALL_VERSION")
    assert(actualLong == 0)
    actualLong = ResolveTableStoreBinlogUDF.getActualValue[Long](10000L, "UNKNOWN")
    assert(actualLong == 0)

    var actualStr = ResolveTableStoreBinlogUDF.getActualValue[String]("test_str", "PUT")
    assert(actualStr equals "test_str")
    actualStr = ResolveTableStoreBinlogUDF.getActualValue[String]("test_str", "DELETE_ONE_VERSION")
    assert(actualStr == null)
    actualStr = ResolveTableStoreBinlogUDF.getActualValue[String]("test_str", "DELETE_ALL_VERSION")
    assert(actualStr == null)
    actualStr = ResolveTableStoreBinlogUDF.getActualValue[String]("test_str", "UNKNOWN")
    assert(actualStr == null)
  }
}
