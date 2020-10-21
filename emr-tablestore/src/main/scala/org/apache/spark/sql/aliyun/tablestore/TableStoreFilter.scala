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

import java.{util => ju}

import scala.collection.JavaConverters._

import com.alicloud.openservices.tablestore.ecosystem.{Filter => OTSFilter}
import com.alicloud.openservices.tablestore.ecosystem.Filter.{CompareOperator, LogicOperator}
import com.alicloud.openservices.tablestore.model.ColumnValue

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

object TableStoreFilter extends Logging {
  def buildFilters(filters: Array[Filter], relation: TableStoreRelation): OTSFilter = {
    val filter = filters.reduceOption[Filter](And)
    val ret = filter.map(buildFilter(_, relation)).getOrElse(OTSFilter.emptyFilter())
    logInfo(s"build all filters: ${ret.toString}")
    ret
  }

  def buildFilter(filter: Filter, relation: TableStoreRelation): OTSFilter = {
    // Match with tablestore ecosystem filter
    val schema = relation.schema
    buildFilter(filter, schema)
  }

  def buildFilter(filter: Filter, schema: StructType): OTSFilter = {
    val f = filter match {
      case And(left, right) =>
        and(buildFilter(left, schema), buildFilter(right, schema))
      case Not(And(left, right)) =>
        or(buildFilter(Not(left), schema), buildFilter(Not(right), schema))
      case Or(left, right) =>
        or(buildFilter(left, schema), buildFilter(right, schema))
      case Not(Or(left, right)) =>
        and(buildFilter(Not(left), schema), buildFilter(Not(right), schema))
      case EqualTo(attribute, value) =>
        convertToOtsFilter(CompareOperator.EQUAL, attribute, value, schema)
      case Not(EqualTo(attribute, value)) =>
        convertToOtsFilter(CompareOperator.NOT_EQUAL, attribute, value, schema)
      case GreaterThan(attribute, value) =>
        convertToOtsFilter(CompareOperator.GREATER_THAN, attribute, value, schema)
      case Not(GreaterThan(attribute, value)) =>
        convertToOtsFilter(CompareOperator.LESS_EQUAL, attribute, value, schema)
      case GreaterThanOrEqual(attribute, value) =>
        convertToOtsFilter(CompareOperator.GREATER_EQUAL, attribute, value, schema)
      case Not(GreaterThanOrEqual(attribute, value)) =>
        convertToOtsFilter(CompareOperator.LESS_THAN, attribute, value, schema)
      case LessThan(attribute, value) =>
        convertToOtsFilter(CompareOperator.LESS_THAN, attribute, value, schema)
      case Not(LessThan(attribute, value)) =>
        convertToOtsFilter(CompareOperator.GREATER_EQUAL, attribute, value, schema)
      case LessThanOrEqual(attribute, value) =>
        convertToOtsFilter(CompareOperator.LESS_EQUAL, attribute, value, schema)
      case Not(LessThanOrEqual(attribute, value)) =>
        convertToOtsFilter(CompareOperator.GREATER_THAN, attribute, value, schema)
      case StringStartsWith(attribute, value) =>
        convertToOtsFilter(CompareOperator.START_WITH, attribute, value, schema)
      case In(attribute, values) =>
        convertToOtsFilter(CompareOperator.IN, attribute, values, schema)
      case IsNull(attribute) =>
        convertToOtsFilter(CompareOperator.IS_NULL, attribute);
      case _ =>
        OTSFilter.emptyFilter
    }

    f
  }

  def and(left: OTSFilter, right: OTSFilter): OTSFilter = {
    if (left.getCompareOperator == CompareOperator.EMPTY_FILTER) {
      right
    } else if (right.getCompareOperator == CompareOperator.EMPTY_FILTER) {
      left
    } else {
      new OTSFilter(LogicOperator.AND, ju.Arrays.asList(left, right))
    }
  }

  def or(left: OTSFilter, right: OTSFilter): OTSFilter = {
    new OTSFilter(LogicOperator.OR, ju.Arrays.asList(left, right))
  }

  def convertToOtsFilter(co: CompareOperator, attribute: String,
                         value: Any, schema: StructType): OTSFilter = {
    val dataType = schema(attribute).dataType
    new OTSFilter(co, attribute, convertToColumnValue(value, dataType))
  }

  // for "IS_NULL" "IS_NOT_NULL"
  def convertToOtsFilter(co: CompareOperator, attribute: String): OTSFilter = {
    new OTSFilter(co, attribute)
  }

  // for "IN"
  def convertToOtsFilter(co: CompareOperator, attribute: String,
                         values: Array[Any], schema: StructType): OTSFilter = {
    val dataType = schema(attribute).dataType
    val columnValueArray: Array[ColumnValue] = values.map(x => convertToColumnValue(x, dataType));
    val list: java.util.List[ColumnValue] = columnValueArray.toList.asJava
    new OTSFilter(co, attribute, list)
  }

  def convertToColumnValue(value: Any, dataType: DataType): ColumnValue = {
    dataType match {
      case LongType =>
        ColumnValue.fromLong(value.asInstanceOf[Long])
      case IntegerType =>
        ColumnValue.fromLong(value.asInstanceOf[Int].toLong)
      case FloatType =>
        ColumnValue.fromDouble(value.asInstanceOf[Float].toDouble)
      case DoubleType =>
        ColumnValue.fromDouble(value.asInstanceOf[Double])
      case ShortType =>
        ColumnValue.fromLong(value.asInstanceOf[Short].toLong)
      case ByteType =>
        ColumnValue.fromLong(value.asInstanceOf[Byte].toLong)
      case StringType =>
        ColumnValue.fromString(value.asInstanceOf[String])
      case BinaryType =>
        ColumnValue.fromBinary(value.asInstanceOf[Array[Byte]])
      case BooleanType =>
        ColumnValue.fromBoolean(value.asInstanceOf[Boolean])
      case _ =>
        throw new IllegalArgumentException(s"unknown data type of ${dataType}")
    }
  }
}
