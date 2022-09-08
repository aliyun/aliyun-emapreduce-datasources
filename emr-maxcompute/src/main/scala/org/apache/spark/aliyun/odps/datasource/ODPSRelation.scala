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
package org.apache.spark.aliyun.odps.datasource

import scala.collection.JavaConverters._

import com.aliyun.odps.PartitionSpec

import org.apache.spark.SparkException
import org.apache.spark.aliyun.odps.datasource.ODPSRelation.OperatorMode
import org.apache.spark.aliyun.odps.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

case class ODPSRelation(options: ODPSOptions)(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan with Logging {

  if (!checkTableExist()) {
    throw new SparkException(s"Table ${options.project}.${options.table} didn't exist.")
  }

  @transient
  private lazy val tableSchema = options.odpsUtil.getTableSchema(options.project, options.table)

  override val needConversion: Boolean = false

  override val schema: StructType = StructType(
    (tableSchema.getColumns.asScala ++ tableSchema.getPartitionColumns.asScala).map { column =>
      OdpsUtils.getCatalystType(column.getName, column.getTypeInfo, column.isNullable)
    }
  )

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredSchema = StructType(requiredColumns.map(c => schema.fields(schema.fieldIndex(c))))
    val odpsUtils = options.odpsUtil

    val requiredPartition = if (odpsUtils.isPartitionTable(options.project, options.table)) {
      val partitionColumnNames = tableSchema.getPartitionColumns.asScala.map(_.getName).toSet
      val partitionColumnFilters = filters.filter { f =>
        val uniqueColumns = f.references.toSet
        uniqueColumns.nonEmpty && uniqueColumns.subsetOf(partitionColumnNames)
      }.toSet

      val customPartitionSpec = options.partitionSpecs.values
      val tablePartitionSpec = odpsUtils.getAllPartitionSpecs(options.project, options.table)

      (customPartitionSpec ++ tablePartitionSpec).filter { spec =>
        partitionColumnFilters.forall { filter => filterPartition(spec, filter) }
      }.mkString(",")
    } else {
      null
    }

    new ODPSRDD(sqlContext.sparkContext, requiredSchema, requiredPartition, options)
      .asInstanceOf[RDD[Row]]
  }

  override def toString: String = {
    // credentials should not be included in the plan output, table information is sufficient.
    s"ODPSRelation(${options.project}.${options.table})"
  }

  private def checkTableExist(): Boolean = {
    options.odpsUtil.tableExist(options.project, options.table)
  }

  private def filterPartition(odpsPartition: PartitionSpec, f: Filter): Boolean = {
    f match {
      case EqualTo(attribute, value) =>
        check(odpsPartition.get(attribute), value, OperatorMode.EQUAL)
      case EqualNullSafe(attribute, value) =>
        val partitionValue = odpsPartition.get(attribute)
        if (partitionValue != null && value != null) {
          check(partitionValue, value, OperatorMode.EQUAL)
        } else {
          partitionValue == null && value == null
        }
      case GreaterThan(attribute, value) =>
        check(odpsPartition.get(attribute), value, OperatorMode.GREATER)
      case GreaterThanOrEqual(attribute, value) =>
        check(odpsPartition.get(attribute), value, OperatorMode.GREATER_OR_EQUAL)
      case LessThan(attribute, value) =>
        check(odpsPartition.get(attribute), value, OperatorMode.LESS)
      case LessThanOrEqual(attribute, value) =>
        check(odpsPartition.get(attribute), value, OperatorMode.LESS_OR_EQUAL)
      case In(attribute, value) =>
        value.exists(v => check(odpsPartition.get(attribute), v, OperatorMode.EQUAL))
      case IsNull(attribute) =>
        odpsPartition.get(attribute) == null // todo: need to think again.
      case IsNotNull(attribute) =>
        odpsPartition.get(attribute) != null // todo: need to think again.
      case And(left, right) =>
        filterPartition(odpsPartition, left) && filterPartition(odpsPartition, right)
      case Or(left, right) =>
        filterPartition(odpsPartition, left) || filterPartition(odpsPartition, right)
      case Not(child) =>
        !filterPartition(odpsPartition, child)
      case StringStartsWith(attribute, value) =>
        odpsPartition.get(attribute).startsWith(value)
      case StringEndsWith(attribute, value) =>
        odpsPartition.get(attribute).endsWith(value)
      case StringContains(attribute, value) =>
        odpsPartition.get(attribute).contains(value)
      case other =>
        throw new UnsupportedOperationException(s"Unknown filter $other")
    }
  }

  private def check(left: String, right: Any, mode: OperatorMode.OperatorMode): Boolean =
    (right, mode) match {
      case (booleanValue: Boolean, OperatorMode.EQUAL) => left.toBoolean == booleanValue
      case (booleanValue: Boolean, OperatorMode.GREATER) => left.toBoolean > booleanValue
      case (booleanValue: Boolean, OperatorMode.GREATER_OR_EQUAL) => left.toBoolean >= booleanValue
      case (booleanValue: Boolean, OperatorMode.LESS) => left.toBoolean < booleanValue
      case (booleanValue: Boolean, OperatorMode.LESS_OR_EQUAL) => left.toBoolean <= booleanValue

      case (byteValue: Byte, OperatorMode.EQUAL) => left.toByte == byteValue
      case (byteValue: Byte, OperatorMode.GREATER) => left.toByte > byteValue
      case (byteValue: Byte, OperatorMode.GREATER_OR_EQUAL) => left.toByte >= byteValue
      case (byteValue: Byte, OperatorMode.LESS) => left.toByte < byteValue
      case (byteValue: Byte, OperatorMode.LESS_OR_EQUAL) => left.toByte <= byteValue

      case (doubleValue: Double, OperatorMode.EQUAL) => left.toDouble == doubleValue
      case (doubleValue: Double, OperatorMode.GREATER) => left.toDouble > doubleValue
      case (doubleValue: Double, OperatorMode.GREATER_OR_EQUAL) => left.toDouble >= doubleValue
      case (doubleValue: Double, OperatorMode.LESS) => left.toDouble < doubleValue
      case (doubleValue: Double, OperatorMode.LESS_OR_EQUAL) => left.toDouble <= doubleValue

      case (floatValue: Float, OperatorMode.EQUAL) => left.toFloat == floatValue
      case (floatValue: Float, OperatorMode.GREATER) => left.toFloat > floatValue
      case (floatValue: Float, OperatorMode.GREATER_OR_EQUAL) => left.toFloat >= floatValue
      case (floatValue: Float, OperatorMode.LESS) => left.toFloat < floatValue
      case (floatValue: Float, OperatorMode.LESS_OR_EQUAL) => left.toFloat <= floatValue

      case (intValue: Int, OperatorMode.EQUAL) => left.toInt == intValue
      case (intValue: Int, OperatorMode.GREATER) => left.toInt > intValue
      case (intValue: Int, OperatorMode.GREATER_OR_EQUAL) => left.toInt >= intValue
      case (intValue: Int, OperatorMode.LESS) => left.toInt < intValue
      case (intValue: Int, OperatorMode.LESS_OR_EQUAL) => left.toInt <= intValue

      case (longValue: Long, OperatorMode.EQUAL) => left.toLong == longValue
      case (longValue: Long, OperatorMode.GREATER) => left.toLong > longValue
      case (longValue: Long, OperatorMode.GREATER_OR_EQUAL) => left.toLong >= longValue
      case (longValue: Long, OperatorMode.LESS) => left.toLong < longValue
      case (longValue: Long, OperatorMode.LESS_OR_EQUAL) => left.toLong <= longValue

      case (shortValue: Short, OperatorMode.EQUAL) => left.toShort == shortValue
      case (shortValue: Short, OperatorMode.GREATER) => left.toShort > shortValue
      case (shortValue: Short, OperatorMode.GREATER_OR_EQUAL) => left.toShort >= shortValue
      case (shortValue: Short, OperatorMode.LESS) => left.toShort < shortValue
      case (shortValue: Short, OperatorMode.LESS_OR_EQUAL) => left.toShort <= shortValue

      case (stringValue: String, OperatorMode.EQUAL) => left == stringValue
      case (stringValue: String, OperatorMode.GREATER) => left > stringValue
      case (stringValue: String, OperatorMode.GREATER_OR_EQUAL) => left >= stringValue
      case (stringValue: String, OperatorMode.LESS) => left < stringValue
      case (stringValue: String, OperatorMode.LESS_OR_EQUAL) => left <= stringValue

      case (other, _) =>
        throw new UnsupportedOperationException(s"Unknown data type for $other .")
    }
}

object ODPSRelation {
  private object OperatorMode extends Enumeration {
    type OperatorMode = Value
    val EQUAL, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL = Value
  }
}
