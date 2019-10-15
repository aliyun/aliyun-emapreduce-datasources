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

import org.apache.spark.sql.aliyun.tablestore.TableStoreSourceProvider.isDefaultField
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.FunSuite

class TableStoreDataSuite extends FunSuite {
  private val testUtils = new TableStoreTestUtil()

  private val testSchema = {
    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog)
      )
    TableStoreSource.tableStoreSchema(TableStoreCatalog(options).schema)
  }

  test("test tablestore data encoder") {
    println(testSchema)
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
    encoderForDataColumns.toRow(new GenericRow(data.toArray))
  }
}
