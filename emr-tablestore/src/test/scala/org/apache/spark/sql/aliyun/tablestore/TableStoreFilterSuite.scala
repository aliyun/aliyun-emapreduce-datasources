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

import scala.util.Random

import com.alicloud.openservices.tablestore.ecosystem.Filter.{CompareOperator, LogicOperator}
import com.alicloud.openservices.tablestore.model.ColumnType

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Not, Or, StringStartsWith}
import org.apache.spark.sql.types._

class TableStoreFilterSuite extends SparkFunSuite {
  private val testUtils = new TableStoreTestUtil()

  test("convert to column value") {
    val longVal: Any = Long.MaxValue
    var columnVal = TableStoreFilter.convertToColumnValue(longVal, LongType)
    assert(columnVal.getValue == longVal)
    assert(columnVal.getType == ColumnType.INTEGER)

    val intVal: Any = Int.MaxValue
    columnVal = TableStoreFilter.convertToColumnValue(intVal, IntegerType)
    assert(columnVal.getValue == intVal)
    assert(columnVal.getType == ColumnType.INTEGER)

    val floatVal: Any = Float.MaxValue
    columnVal = TableStoreFilter.convertToColumnValue(floatVal, FloatType)
    assert(columnVal.getValue == floatVal)
    assert(columnVal.getType == ColumnType.DOUBLE)

    val doubleVal: Any = Double.MaxValue
    columnVal = TableStoreFilter.convertToColumnValue(doubleVal, DoubleType)
    assert(columnVal.getValue == doubleVal)
    assert(columnVal.getType == ColumnType.DOUBLE)

    val shortVal: Any = Short.MaxValue
    columnVal = TableStoreFilter.convertToColumnValue(shortVal, ShortType)
    assert(columnVal.getValue == shortVal)
    assert(columnVal.getType == ColumnType.INTEGER)

    val byteVal: Any = Byte.MaxValue
    columnVal = TableStoreFilter.convertToColumnValue(byteVal, ByteType)
    assert(columnVal.getValue == byteVal)
    assert(columnVal.getType == ColumnType.INTEGER)

    val stringVal: Any = "TestString"
    columnVal = TableStoreFilter.convertToColumnValue(stringVal, StringType)
    assert(columnVal.getValue == stringVal)
    assert(columnVal.getType == ColumnType.STRING)

    val bytesVal: Any = "TestBytes".getBytes()
    columnVal = TableStoreFilter.convertToColumnValue(bytesVal, BinaryType)
    assert(columnVal.getValue == bytesVal)
    assert(columnVal.getType == ColumnType.BINARY)

    val boolVal: Any = false
    columnVal = TableStoreFilter.convertToColumnValue(boolVal, BooleanType)
    assert(columnVal.getValue == boolVal)
    assert(columnVal.getType == ColumnType.BOOLEAN)

    // unknown
    try {
      columnVal = TableStoreFilter.convertToColumnValue("unknown", NullType)
    } catch {
      case ex: IllegalArgumentException =>
        succeed
      case _ =>
        fail
    }
  }

  test("build single filter") {
    val schema = testUtils.createTestStructType()
    val (key, value) = ("PkString", "110")
    var filter: Filter = EqualTo(key, value)
    var otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.EQUAL))

    filter = GreaterThan(key, value)
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.GREATER_THAN))

    filter = GreaterThanOrEqual(key, value)
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.GREATER_EQUAL))

    filter = LessThan(key, value)
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.LESS_THAN))

    filter = LessThanOrEqual(key, value)
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.LESS_EQUAL))

    filter = StringStartsWith(key, value)
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.START_WITH))
  }

  test("build single Not filter") {
    val schema = testUtils.createTestStructType()
    val (key, value) = ("PkString", "110")
    var filter = Not(EqualTo(key, value))
    var otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.NOT_EQUAL))

    filter = Not(GreaterThan(key, value))
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.LESS_EQUAL))

    filter = Not(GreaterThanOrEqual(key, value))
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.LESS_THAN))

    filter = Not(LessThan(key, value))
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.GREATER_EQUAL))

    filter = Not(LessThanOrEqual(key, value))
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getColumnName.equals(key))
    assert(otsFilter.getColumnValue.getValue.equals(value))
    assert(otsFilter.getCompareOperator.equals(CompareOperator.GREATER_THAN))

    filter = Not(StringStartsWith(key, value))
    otsFilter = TableStoreFilter.buildFilter(filter, schema)
    assert(otsFilter.getCompareOperator.equals(CompareOperator.EMPTY_FILTER))
  }

  test("random build And filter") {
    val schema = testUtils.createTestStructType()
    val filters = Array(EqualTo, GreaterThan, GreaterThanOrEqual, LessThan,
      LessThanOrEqual, StringStartsWith)
    val rand = new Random(System.currentTimeMillis())
    val (key, value) = ("PkString", rand.nextInt(10000).toString)
    val lft = filters(rand.nextInt(filters.length))(key, value)
    val rgt = filters(rand.nextInt(filters.length))(key, value)
    val otsFilter = TableStoreFilter.buildFilter(And(lft, rgt), schema)

    assert(otsFilter.getLogicOperator.equals(LogicOperator.AND))
    assert(otsFilter.getColumnName == null)
    assert(otsFilter.getColumnValue == null)
    assert(otsFilter.isNested)
    assert(otsFilter.getSubFilters.size == 2)
    assert(otsFilter.getSubFilters.get(0).getColumnName.equals(key))
    assert(otsFilter.getSubFilters.get(0).getColumnValue.getValue.equals(value))
    val lftFilter = TableStoreFilter.buildFilter(lft, schema)
    assert(otsFilter.getSubFilters.get(0).getCompareOperator.equals(lftFilter.getCompareOperator))
    assert(otsFilter.getSubFilters.get(1).getColumnName.equals(key))
    assert(otsFilter.getSubFilters.get(1).getColumnValue.getValue.equals(value))
    val rgtFilter = TableStoreFilter.buildFilter(rgt, schema)
    assert(otsFilter.getSubFilters.get(1).getCompareOperator.equals(rgtFilter.getCompareOperator))
  }

  test("random build Or filter") {
    val schema = testUtils.createTestStructType()
    val filters = Array(EqualTo, GreaterThan, GreaterThanOrEqual, LessThan,
      LessThanOrEqual, StringStartsWith)
    val rand = new Random(System.currentTimeMillis())
    val (key, value) = ("PkString", rand.nextInt(10000).toString)
    val lft = filters(rand.nextInt(filters.length))(key, value)
    val rgt = filters(rand.nextInt(filters.length))(key, value)
    val otsFilter = TableStoreFilter.buildFilter(Or(lft, rgt), schema)

    assert(otsFilter.getLogicOperator.equals(LogicOperator.OR))
    assert(otsFilter.getColumnName == null)
    assert(otsFilter.getColumnValue == null)
    assert(otsFilter.isNested)
    assert(otsFilter.getSubFilters.size == 2)
    assert(otsFilter.getSubFilters.get(0).getColumnName.equals(key))
    assert(otsFilter.getSubFilters.get(0).getColumnValue.getValue.equals(value))
    val lftFilter = TableStoreFilter.buildFilter(lft, schema)
    assert(otsFilter.getSubFilters.get(0).getCompareOperator.equals(lftFilter.getCompareOperator))
    assert(otsFilter.getSubFilters.get(1).getColumnName.equals(key))
    assert(otsFilter.getSubFilters.get(1).getColumnValue.getValue.equals(value))
    val rgtFilter = TableStoreFilter.buildFilter(rgt, schema)
    assert(otsFilter.getSubFilters.get(1).getCompareOperator.equals(rgtFilter.getCompareOperator))
  }

  test("build complex filter") {
    val schema = testUtils.createTestStructType()
    val greaterFilter = GreaterThan("PkString", "testStr")
    val equalFilter = EqualTo("PkInt", 666666L)
    val lessFilter = LessThanOrEqual("col5", 3.1415)
    val lft = And(greaterFilter, equalFilter)
    val rgt = Or(lessFilter, Not(equalFilter))
    val otsFilter = TableStoreFilter.buildFilter(And(lft, rgt), schema)

    assert(otsFilter.isNested)
    assert(otsFilter.getSubFilters.size == 2)
    assert(otsFilter.getLogicOperator.equals(LogicOperator.AND))
    val left = otsFilter.getSubFilters.get(0)
    assert(left.isNested)
    assert(left.getSubFilters.size == 2)
    assert(left.getLogicOperator.equals(LogicOperator.AND))
    assert(left.getSubFilters.get(0).getColumnName.equals("PkString"))
    assert(left.getSubFilters.get(0).getColumnValue.getValue.equals("testStr"))
    assert(left.getSubFilters.get(0).getCompareOperator.equals(CompareOperator.GREATER_THAN))
    assert(left.getSubFilters.get(1).getColumnName.equals("PkInt"))
    assert(left.getSubFilters.get(1).getColumnValue.getValue.asInstanceOf[Long] == 666666)
    assert(left.getSubFilters.get(1).getCompareOperator.equals(CompareOperator.EQUAL))

    val right = otsFilter.getSubFilters.get(1)
    assert(right.isNested)
    assert(right.getSubFilters.size == 2)
    assert(right.getLogicOperator.equals(LogicOperator.OR))
    assert(right.getSubFilters.get(0).getColumnName.equals("col5"))
    assert(right.getSubFilters.get(0).getColumnValue.getValue.asInstanceOf[Double] == 3.1415)
    assert(right.getSubFilters.get(0).getCompareOperator.equals(CompareOperator.LESS_EQUAL))
    assert(right.getSubFilters.get(1).getColumnName.equals("PkInt"))
    assert(right.getSubFilters.get(1).getColumnValue.getValue.asInstanceOf[Long] == 666666)
    assert(right.getSubFilters.get(1).getCompareOperator.equals(CompareOperator.NOT_EQUAL))
  }
}

