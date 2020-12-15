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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.aliyun.tablestore.TableStoreSourceProvider.isDefaultField
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow

class TableStoreDataSuite extends SparkFunSuite {
  private val testUtils = new TableStoreTestUtil()

  private val testSchema = {
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> TableStoreTestUtil.catalog)
      )
    TableStoreSource.tableStoreSchema(TableStoreCatalog(options).schema)
  }

  test("test tablestore data encoder") {
    val schemaFieldPos: Map[String, Int] = testSchema.fieldNames
      .filter(
        fieldName => !isDefaultField(fieldName)
      )
      .zipWithIndex
      .toMap
    val schemaFieldPosSize = schemaFieldPos.size
    val columnArray = Array.tabulate(schemaFieldPosSize)(
      _ => (null, null).asInstanceOf[(String, Any)]
    )
    columnArray(4) = ("col1", null)
    //    val td = Array(("PkString", "1"), (null, null), ("PkInt", 2), (null, null), ("col1", ""))
    val data = new SchemaTableStoreData("PUT", 12345678, columnArray)
    val encoderForDataColumns = RowEncoder(testSchema).resolveAndBind()
    encoderForDataColumns.createSerializer().apply(new GenericRow(data.toArray))
  }
}
