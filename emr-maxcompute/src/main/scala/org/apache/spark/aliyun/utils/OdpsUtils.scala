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
package org.apache.spark.aliyun.utils

import java.math.BigDecimal
import java.util

import scala.collection.JavaConverters._

import com.aliyun.odps._
import com.aliyun.odps.`type`._
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.{Binary, Char, SimpleStruct, Varchar}
import com.aliyun.odps.task.SQLTask
import com.aliyun.odps.tunnel.TableTunnel

import org.apache.spark.aliyun.odps.OdpsPartition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class OdpsUtils(odps: Odps, tunnel: TableTunnel) extends Logging {
  import OdpsUtils._

  def getTableTunnel: TableTunnel = tunnel

  /**
   * Drop specific partition of ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param pname The name of ODPS table partition, if partitioned table.
   * @return Success or not.
   */
  def dropPartition(
                     project: String,
                     table: String,
                     pname: String): Boolean = {
    try {
      odps.setDefaultProject(project)
      odps.tables().get(project, table).deletePartition(new PartitionSpec(pname), true)
      true
    } catch {
      case e: OdpsException =>
        logError(s"somethings wrong happens when delete partition $pname of $table.", e)
        false
    }
  }

  /**
   * Drop specific ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return Success or not.
   */
  def dropTable(project: String, table: String): Boolean = {
    try {
      odps.setDefaultProject(project)
      odps.tables().delete(project, table, true)
      true
    } catch {
      case e: OdpsException =>
        logError(s"somethings wrong happens when delete table $table.", e)
        false
    }
  }

  /**
   * Create ODPS table.
   *
   * @param project Name of odps project.
   * @param table Name of odps table.
   * @param schema refer to {{TableSchema}}.
   * @param ifNotExists Fail or not if target table exists.
   */
  def createTable(
                   project: String,
                   table: String,
                   schema: TableSchema,
                   ifNotExists: Boolean): Unit = {
    odps.setDefaultProject(project)
    odps.tables().create(project, table, schema, ifNotExists)
  }

  /**
   * Create specific partition of ODPS table.
   *
   * @param project The name of ODPS project.
   * @param table   The name of ODPS table.
   * @param partition   The name of ODPS table partition, if partitioned table.
   * @return Success or not.
   */
  def createPartition(project: String, table: String, partition: PartitionSpec): Boolean = {
    try {
      odps.setDefaultProject(project)
      odps.tables().get(project, table).createPartition(partition, true)
      true
    } catch {
      case e: OdpsException =>
        logError(s"somethings wrong happens when create table $table partition $partition.", e)
        false
    }
  }

  def getRecordCount(project: String, table: String): Long = {
    odps.setDefaultProject(project)

    val start = System.nanoTime()

    val session = tunnel.createDownloadSession(project, table)
    val numRecords = session.getRecordCount

    val end = System.nanoTime()
    logInfo(s"##### time usage: ${end - start} ns.")

    numRecords
  }

  def getRecordCount(project: String, table: String, partitionSpec: String): Long = {
    odps.setDefaultProject(project)

    val start = System.nanoTime()

    val spec = new PartitionSpec(partitionSpec)

    val session = tunnel.createDownloadSession(project, table, spec)
    val numRecords = session.getRecordCount

    val end = System.nanoTime()
    logInfo(s"##### time usage: ${end - start} ns.")

    numRecords
  }

  /**
   * Get the table schema of ODPS table
   * @param project the name of ODPS project
   * @param table the name of ODPS table
   * @return a tableSchema
   */
  def getTableSchema(project: String, table: String): TableSchema = {
    odps.setDefaultProject(project)
    odps.tables().get(project, table).getSchema
  }

  /**
   * Get the table schema of ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param isPartition Is partition column or not.
   * @return
   */
  def getTableSchema(
                      project: String,
                      table: String,
                      isPartition: Boolean): Array[(String, TypeInfo)] = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(project, table).getSchema
    val columns = schema.getColumns
    if (isPartition) {
      columns.addAll(schema.getPartitionColumns)
    }
    columns.asScala.map(e => (e.getName, e.getTypeInfo)).toArray
  }

  /**
   * Get information of specific column via column name.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param name The name of specific column.
   * @return Column index and type.
   */
  def getColumnByName(project: String, table: String, name: String): (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(project, table).getSchema
    val idx = schema.getColumnIndex(name)
    val colType = schema.getColumn(name).getTypeInfo
    val field = getCatalystType(name, colType, true)

    (idx.toString, field.dataType.simpleString)
  }

  /**
   * Get information of specific column via column index.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @param idx The index of specific column.
   * @return Column name and type.
   */
  def getColumnByIdx(project: String, table: String, idx: Int): (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(project, table).getSchema
    val column = schema.getColumn(idx)
    val name = column.getName
    val colType = schema.getColumn(name).getTypeInfo
    val field = getCatalystType(name, colType, true)

    (name, field.dataType.simpleString)
  }

  /**
   * Run sql on ODPS.
   *
   * @param project The name of ODPS project.
   * @param sqlCmd An ODPS sql
   * @return An instance of ODPS.
   */
  def runSQL(project: String, sqlCmd: String, hints: Map[String, String] = Map.empty): Instance = {
    odps.setDefaultProject(project)
    log.info("SQL command: " + sqlCmd)
    try {
      import scala.collection.JavaConverters._
      SQLTask.run(odps, project, sqlCmd, hints.asJava, null)
    } catch {
      case e: OdpsException => e.printStackTrace(); null
    }
  }

  def getAllPartitions(project: String, table: String): Iterator[String] = {
    odps.setDefaultProject(project)
    odps.tables().get(project, table)
      .getPartitionSpecs.asScala
      .map(_.toString(false, true))
      .toIterator
  }

  /**
   * Get all partition [[PartitionSpec]] of specific ODPS table.
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return All partition [[PartitionSpec]]
   */
  def getAllPartitionSpecs(project: String, table: String): Iterator[PartitionSpec] = {
    odps.setDefaultProject(project)
    odps.tables().get(project, table).getPartitionSpecs.asScala.toIterator
  }

  /**
   * Check if the table is a partition table
   *
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return
   */
  def isPartitionTable(project: String, table: String): Boolean = {
    odps.setDefaultProject(project)
    odps.tables().get(project, table).isPartitioned
  }

  /**
   * Check if the table exists
   *
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return
   */
  def tableExist(project: String, table: String): Boolean = {
    odps.setDefaultProject(project)
    odps.tables().exists(project, table)
  }

  /**
   * Check if the partition exists in the table,
   *
   * `partitionSpec` like `pt='xxx',ds='yyy'`
   * @param project The name of ODPS project.
   * @param table The name of ODPS table.
   * @return
   */
  def partitionExist(project: String, table: String, partitionSpec: String): Boolean = {
    odps.setDefaultProject(project)
    odps.tables().get(project, table).hasPartition(new PartitionSpec(partitionSpec))
  }

}

object OdpsUtils {
  def apply(
             accessKeyId: String,
             accessKeySecret: String,
             odpsUrl: String,
             tunnelUrl: String): OdpsUtils = {

    val account = new AliyunAccount(accessKeyId, accessKeySecret)
    val odps = new Odps(account)
    odps.setEndpoint(odpsUrl)

    val tunnel = new TableTunnel(odps)
    tunnel.setEndpoint(tunnelUrl)

    new OdpsUtils(odps, tunnel)
  }

  def apply(split: OdpsPartition): OdpsUtils = {
    apply(split.accessKeyId, split.accessKeySecret, split.odpsUrl, split.tunnelUrl)
  }

  def getCatalystType(columnName: String, columnType: TypeInfo, nullable: Boolean): StructField = {
    val metadata = new MetadataBuilder()
      .putString("name", columnName)
      .putLong("scale", 0L)

    StructField(columnName, typeInfo2Type(columnType), nullable, metadata.build())
  }

  private val ODPS_DECIMAL_DEFAULT_PRECISION = 38
  private val ODPS_DECIMAL_DEFAULT_SCALE = 18

  // convert from Spark DataType to Odps DataType
  def sparkData2OdpsData(t: TypeInfo): Object => AnyRef = {
    t.getOdpsType match {
      case OdpsType.BOOLEAN => v: Object => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => v: Object => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => v: Object => v.asInstanceOf[java.lang.Long]
      case OdpsType.DATETIME => v: Object =>
        if (v != null) new java.util.Date(v.asInstanceOf[java.sql.Date].getTime)
        else null
      case OdpsType.STRING => v: Object =>
        if (v != null) v.asInstanceOf[String]
        else null
      case OdpsType.DECIMAL => v: Object =>
        val ti = t.asInstanceOf[DecimalTypeInfo]
        if (v != null) v.asInstanceOf[BigDecimal].setScale(ti.getScale)
        else null
      case OdpsType.VARCHAR => v: Object =>
        val ti = t.asInstanceOf[VarcharTypeInfo]
        if (v != null) new Varchar(v.asInstanceOf[UTF8String].toString, ti.getLength)
        else null
      case OdpsType.CHAR => v: Object =>
        val ti = t.asInstanceOf[CharTypeInfo]
        if (v != null) new Char(v.asInstanceOf[UTF8String].toString, ti.getLength)
        else null
      case OdpsType.DATE => v: Object =>
        if (v != null) new java.sql.Date(v.asInstanceOf[Int].toLong * (3600 * 24 * 1000))
        else null
      case OdpsType.TIMESTAMP => v: Object =>
        if (v != null) v.asInstanceOf[java.sql.Timestamp]
        else null
      case OdpsType.FLOAT => v: Object => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => v: Object => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => v: Object => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => v: Object => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => v: Object =>
        val ti = t.asInstanceOf[ArrayTypeInfo]
        if (v != null) {
          if (v.isInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]) {
            v.asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData]
              .toArray[Object](typeInfo2Type(ti.getElementTypeInfo))
              .map(e => sparkData2OdpsData(ti.getElementTypeInfo)(e)).toList.asJava
          } else if (v.isInstanceOf[Seq[Any]]) {
            v.asInstanceOf[Seq[Any]].map(e =>
              sparkData2OdpsData(ti.getElementTypeInfo)(e.asInstanceOf[Object])).toList.asJava
          } else null
        } else null
      case OdpsType.BINARY => v: Object => new Binary(v.asInstanceOf[Array[Byte]])
      case OdpsType.MAP => v: Object =>
        val ti = t.asInstanceOf[MapTypeInfo]
        if (v != null) {
          val m = new java.util.HashMap[Object, Object]
          if (v.isInstanceOf[org.apache.spark.sql.catalyst.util.MapData]) {
            val mapData = v.asInstanceOf[org.apache.spark.sql.catalyst.util.MapData]
            mapData.keyArray.toArray[Object](typeInfo2Type(ti.getKeyTypeInfo))
              .zip(
                mapData.valueArray.toArray[Object](
                  typeInfo2Type(ti.getValueTypeInfo)))
              .foreach(p => m.put(
                sparkData2OdpsData(ti.getKeyTypeInfo)(p._1),
                sparkData2OdpsData(ti.getValueTypeInfo)(p._2)
                  .asInstanceOf[Object])
              )
          } else if (v.isInstanceOf[scala.collection.Map[Any, Any]]) {
            val map = v.asInstanceOf[scala.collection.Map[Any, Any]]
            map.keys.foreach(key => {
              m.put(
                sparkData2OdpsData(ti.getKeyTypeInfo)(key.asInstanceOf[Object]),
                sparkData2OdpsData(ti.getValueTypeInfo)(map.get(key).orNull.asInstanceOf[Object])
              )
            })
          }
          m
        } else null
      case OdpsType.STRUCT => v: Object => {
        val ti = t.asInstanceOf[StructTypeInfo]
        if (v != null) {
          if (v.isInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow]) {
            val r = v.asInstanceOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow]
            val l = (0 until r.numFields).zip(ti.getFieldTypeInfos.toArray()).map(p =>
              sparkData2OdpsData(p._2.asInstanceOf[TypeInfo])(r.get(p._1,
                typeInfo2Type(p._2.asInstanceOf[TypeInfo])))
            ).toList.asJava
            new SimpleStruct(ti, l)
          } else if (v.isInstanceOf[org.apache.spark.sql.Row]) {
            val r = v.asInstanceOf[org.apache.spark.sql.Row]
            val l = (0 until r.length).zip(ti.getFieldTypeInfos.toArray()).map(p =>
              sparkData2OdpsData(p._2.asInstanceOf[TypeInfo])(r.get(p._1).asInstanceOf[Object])
            ).toList.asJava
            new SimpleStruct(ti, l)
          } else null
        } else null
      }
    }
  }

  private def sparkPrimitiveTypeToOdpsPrimitiveType(t: DataType): TypeInfo = t match {
    case DataTypes.ByteType => TypeInfoFactory.TINYINT
    case DataTypes.ShortType => TypeInfoFactory.SMALLINT
    case DataTypes.IntegerType => TypeInfoFactory.INT
    case DataTypes.LongType => TypeInfoFactory.BIGINT
    case DataTypes.FloatType => TypeInfoFactory.FLOAT
    case DataTypes.DoubleType => TypeInfoFactory.DOUBLE
    case dt: DecimalType => TypeInfoFactory.getDecimalTypeInfo(dt.precision, dt.scale)
    case DataTypes.StringType => TypeInfoFactory.STRING
    case DataTypes.BinaryType => TypeInfoFactory.BINARY
    case DataTypes.BooleanType => TypeInfoFactory.BOOLEAN
    case DataTypes.TimestampType => TypeInfoFactory.TIMESTAMP
    case DataTypes.DateType => TypeInfoFactory.DATE
    case other =>
      throw new UnsupportedOperationException(
        s"Could not convert Spark type ${other.typeName} to ODPS type.")
  }

  def sparkTypeToOdpsType(t: DataType): TypeInfo = t match {
    case _ @ ArrayType(elementType, _) =>
      TypeInfoFactory.getArrayTypeInfo(sparkPrimitiveTypeToOdpsPrimitiveType(elementType))
    case _ @ MapType(keyType, valueType, _) =>
      TypeInfoFactory.getMapTypeInfo(
        sparkPrimitiveTypeToOdpsPrimitiveType(keyType),
        sparkPrimitiveTypeToOdpsPrimitiveType(valueType))
    case _ @ StructType(fields) =>
      val names = new util.ArrayList[String](fields.length)
      val typeInfos = new util.ArrayList[TypeInfo](fields.length)
      fields.foreach(field => {
        names.add(field.name)
        typeInfos.add(sparkPrimitiveTypeToOdpsPrimitiveType(field.dataType))
      })
      TypeInfoFactory.getStructTypeInfo(names, typeInfos)
    case other => sparkPrimitiveTypeToOdpsPrimitiveType(other)
  }

  def odpsData2SparkData(t: TypeInfo, isDatasource: Boolean = true): Object => Any = {
    val func = t.getOdpsType match {
      case OdpsType.BOOLEAN => (v: Object) => v.asInstanceOf[java.lang.Boolean]
      case OdpsType.DOUBLE => (v: Object) => v.asInstanceOf[java.lang.Double]
      case OdpsType.BIGINT => (v: Object) => v.asInstanceOf[java.lang.Long]
      case OdpsType.DATETIME => (v: Object) =>
        if (!isDatasource) {
          new java.sql.Date(v.asInstanceOf[java.util.Date].getTime)
        } else {
          v.asInstanceOf[java.util.Date].getTime.toInt
        }
      case OdpsType.STRING => (v: Object) => v match {
        case str: String =>
          if (!isDatasource) {
            str
          } else {
            UTF8String.fromString(str)
          }
        case bytes: Array[Byte] =>
          if (!isDatasource) {
            new String(bytes)
          } else {
            UTF8String.fromBytes(bytes)
          }
      }
      case OdpsType.DECIMAL => (v: Object) => {
        val ti = t.asInstanceOf[DecimalTypeInfo]
        if (ti.getPrecision == 54 && ti.getScale == 18) {
          (new Decimal).set(v.asInstanceOf[java.math.BigDecimal],
            ODPS_DECIMAL_DEFAULT_PRECISION, ODPS_DECIMAL_DEFAULT_SCALE)
        } else {
          (new Decimal).set(v.asInstanceOf[java.math.BigDecimal], ti.getPrecision, ti.getScale)
        }
      }
      case OdpsType.VARCHAR => (v: Object) => {
        val varchar = v.asInstanceOf[Varchar]
        UTF8String.fromString(varchar.getValue.substring(0, varchar.length()))
      }
      case OdpsType.CHAR => (v: Object) => {
        val char = v.asInstanceOf[Char]
        UTF8String.fromString(char.getValue.substring(0, char.length()))
      }
      case OdpsType.DATE => (v: Object) =>
        if (!isDatasource) {
          v.asInstanceOf[java.sql.Date]
        } else {
          v.asInstanceOf[java.sql.Date].getTime
        }
      case OdpsType.TIMESTAMP => (v: Object) => {
        if (!isDatasource) {
          v.asInstanceOf[java.sql.Timestamp]
        } else {
          v.asInstanceOf[java.sql.Timestamp].getTime * 1000
        }
      }
      case OdpsType.FLOAT => (v: Object) => v.asInstanceOf[java.lang.Float]
      case OdpsType.INT => (v: Object) => v.asInstanceOf[java.lang.Integer]
      case OdpsType.SMALLINT => (v: Object) => v.asInstanceOf[java.lang.Short]
      case OdpsType.TINYINT => (v: Object) => v.asInstanceOf[java.lang.Byte]
      case OdpsType.ARRAY => (v: Object) => {
        val array = v.asInstanceOf[java.util.ArrayList[Object]]
        if (!isDatasource) {
          array.asScala
        } else {
          new GenericArrayData(array.toArray().
            map(odpsData2SparkData(t.asInstanceOf[ArrayTypeInfo].getElementTypeInfo)(_)))
        }
      }
      case OdpsType.BINARY => (v: Object) => v.asInstanceOf[Binary].data()
      case OdpsType.MAP => (v: Object) => {
        if (!isDatasource) {
          v.asInstanceOf[java.util.HashMap[Object, Object]].asScala
        } else {
          val m = v.asInstanceOf[java.util.HashMap[Object, Object]]
          val keyArray = m.keySet().toArray()
          new ArrayBasedMapData(
            new GenericArrayData(keyArray.
              map(odpsData2SparkData(t.asInstanceOf[MapTypeInfo].getKeyTypeInfo)(_))),
            new GenericArrayData(keyArray.map(m.get(_)).
              map(odpsData2SparkData(t.asInstanceOf[MapTypeInfo].getValueTypeInfo)(_)))
          )
        }
      }
      case OdpsType.STRUCT => (v: Object) => {
        val struct = v.asInstanceOf[com.aliyun.odps.data.Struct]
        if (!isDatasource) {
          Row.fromSeq(struct.getFieldValues.asScala.zipWithIndex
            .map(x => odpsData2SparkData(struct.getFieldTypeInfo(x._2), isDatasource)(x._1)))
        } else {
          org.apache.spark.sql.catalyst.InternalRow
            .fromSeq(struct.getFieldValues.asScala.zipWithIndex
              .map(x => odpsData2SparkData(struct.getFieldTypeInfo(x._2))(x._1)))
        }
      }
    }
    nullSafeEval(func)
  }

  private def nullSafeEval(func: Object => Any): Object => Any =
    (v: Object) => if (v ne null) func(v) else null

  /** Given the string representation of a type, return its DataType */
  def typeInfo2Type(typeInfo: TypeInfo): DataType = {
    typeStr2Type(typeInfo.getTypeName.toLowerCase())
  }

  private def splitTypes(types: String): List[String] = {
    var unclosedAngles = 0
    val sb = new StringBuilder()
    var typeList = List.empty[String]
    types foreach (c => {
      if (c == ',' && unclosedAngles == 0) {
        typeList :+= sb.toString()
        sb.clear()
      } else if (c == '<') {
        unclosedAngles += 1
        sb.append(c)
      } else if (c == '>') {
        unclosedAngles -= 1
        sb.append(c)
      } else {
        sb.append(c)
      }
    })
    typeList :+= sb.toString()
    typeList
  }

  /** Given the string representation of a type, return its DataType */
  def typeStr2Type(typeStr: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    val CHAR = """char\(\s*(\d+)\s*\)""".r
    val VARCHAR = """varchar\(\s*(\d+)\s*\)""".r
    val ARRAY = """array<\s*(.+)\s*>""".r
    val MAP = """map<\s*(.+)\s*>""".r
    val STRUCT = """struct<\s*(.+)\s*>""".r

    typeStr.toLowerCase match {
      case "decimal" => DecimalType(ODPS_DECIMAL_DEFAULT_PRECISION, ODPS_DECIMAL_DEFAULT_SCALE)
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" => BooleanType
      case "datetime" => DateType
      case "date" => DateType
      case "timestamp" => TimestampType
      case "tinyint" => ByteType
      case "smallint" => ShortType
      case "int" => IntegerType
      case "bigint" => LongType
      case "string" => StringType
      case CHAR(_) => StringType
      case VARCHAR(_) => StringType
      case "binary" => BinaryType
      case ARRAY(elemType) => ArrayType(typeStr2Type(elemType))
      case MAP(types) =>
        val List(keyType, valType) = splitTypes(types)
        MapType(typeStr2Type(keyType), typeStr2Type(valType))
      case STRUCT(types) =>
        val elemTypes = splitTypes(types).map(elem => {
          val Array(name, typeStr) = elem.split(":", 2)
          StructField(name, typeStr2Type(typeStr))
        })
        StructType(elemTypes)
      case _ =>
        throw new Exception(s"ODPS data type: $typeStr not supported!")
    }
  }
}
