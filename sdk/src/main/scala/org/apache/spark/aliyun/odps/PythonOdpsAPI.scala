/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.aliyun.odps

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD}
import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.Record
import com.aliyun.odps.OdpsType
import net.razorvine.pickle.{Unpickler, Pickler}
import java.text.SimpleDateFormat
import java.util.{List => JList}
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import scala.collection.mutable

class PythonOdpsAPI(@transient jsc: JavaSparkContext,
                    accessKeyId: String,
                    accessKeySecret: String,
                    odpsUrl: String,
                    tunnelUrl: String) extends Logging with Serializable {

  val odpsOps = OdpsOps(jsc.sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)

  def readTable(project: String,
                table: String,
                partition: String,
                cols: Array[Int],
                bytesCols: Array[Int],
                batchSize: Int,
                numPartition: Int): JavaRDD[Array[Byte]] = {
    val colsLen = odpsOps.getTableSchema(project, table, false).length
    val colsTuple = prepareColsTuple(cols, bytesCols, colsLen)
    val rdd = odpsOps.readTable(project, table, partition, readTransfer(colsTuple), numPartition)
    JavaRDD.fromRDD(serialize(rdd, batchSize))
  }

  def readTable(project: String,
                table: String,
                cols: Array[Int],
                bytesCols: Array[Int],
                batchSize: Int,
                numPartition: Int): JavaRDD[Array[Byte]] = {
    val colsLen = odpsOps.getTableSchema(project, table, false).length
    val colsTuple = prepareColsTuple(cols, bytesCols, colsLen)
    val rdd = odpsOps.readTable(project, table, readTransfer(colsTuple), numPartition)
    JavaRDD.fromRDD(serialize(rdd, batchSize))
  }

  def saveToTable(project: String, table: String, partition: String,
                  pyRdd: JavaRDD[Array[Byte]], cols: Array[Int], bytesCols: Array[Int],
                  batched: Boolean, defaultCreatePartition: Boolean, isOverWrite: Boolean) {
    val colsLen = odpsOps.getTableSchema(project, table, false).length
    val colsTuple = prepareColsTuple(cols, bytesCols, colsLen)
    val rdd = deserialize(pyRdd, batched)
    odpsOps.saveToTable(project, table, partition, rdd, writeTransfer(colsTuple) _, defaultCreatePartition, isOverWrite)
  }

  def saveToTable(project: String, table: String,
                  pyRdd: JavaRDD[Array[Byte]], cols: Array[Int],
                  bytesCols: Array[Int], batched: Boolean) {
    val colsLen = odpsOps.getTableSchema(project, table, false).length
    val colsTuple = prepareColsTuple(cols, bytesCols, colsLen)
    val rdd = deserialize(pyRdd, batched)
    odpsOps.saveToTable(project, table, rdd, writeTransfer(colsTuple))
  }

  private def prepareColsTuple(cols: Array[Int], bytesCols: Array[Int], columnsLen: Int): Array[(Int, Int)] = {
    // 如果col是空数组，说明选择全部的列
    val m1 = new mutable.HashMap[Int, Int]()
    val selectedCols = if (cols.length == 0) {
      Array.range(0, columnsLen)
    } else {
      cols
    }
    selectedCols.foreach(e => m1.put(e, 0))

    // 如果bytesCols是空数组，说明所有被选择的列按照String读取
    val m2 = new mutable.HashMap[Int, Int]()
    val selectedBytesCols = if(bytesCols.length == 0){
      new Array[Int](0)
    } else {
      bytesCols
    }
    selectedBytesCols.foreach(e => m2.put(e, 1))

    (m1.keySet ++ m2.keySet).map { i =>
      val m1Val = m1.getOrElse(i, 0)
      val m2Val = m2.getOrElse(i, 0)
      i -> (m1Val + m2Val)
    }.toArray
  }

  private def readTransfer(colsTuple: Array[(Int, Int)])(record: Record, schema: TableSchema): Array[_] = {
    // 读取表中的每一列转换成一个数组
    colsTuple.sortBy(_._1).map { e =>
      val idx = e._1
      val isBytes = e._2
      val col = schema.getColumn(idx)
      col.getType match {
        case OdpsType.BIGINT => record.getBigint(idx)
        case OdpsType.DOUBLE => record.getDouble(idx)
        case OdpsType.BOOLEAN => record.getBoolean(idx)
        case OdpsType.DATETIME =>
          PythonOdpsAPI.dateFormat.format(record.getDatetime(idx))
        case OdpsType.STRING => if(isBytes == 1) record.getBytes(idx) else record.getString(idx)
      }
    }
  }

  private def writeTransfer(colsTuple: Array[(Int, Int)])(elements: Array[_], record: Record, schema: TableSchema) {
    // 根据数据类型，写入每一列
    colsTuple.sortBy(_._1).zip(elements).foreach { t =>
      val idx = t._1._1
      val isBytes = t._1._2
      val element = t._2
      val col = schema.getColumn(idx)
      col.getType match {
        case OdpsType.BIGINT => record.setBigint(idx, element.toString.toLong)
        case OdpsType.DOUBLE => record.setDouble(idx, element.asInstanceOf[Double])
        case OdpsType.BOOLEAN => record.setBoolean(idx, element.asInstanceOf[Boolean])
        case OdpsType.DATETIME =>
          val date = PythonOdpsAPI.dateFormat.parse(element.asInstanceOf[String])
          record.setDatetime(idx, date)
        case OdpsType.STRING => if(isBytes == 1) record.setString(idx, element.asInstanceOf[Array[Byte]])
        else record.setString(idx, element.asInstanceOf[String])
      }
    }
  }

  def getTableSchema(project: String, table: String, isPartition: Boolean): JList[Array[Byte]] = {
    val arr = odpsOps.getTableSchema(project, table, isPartition)
    val pickle = new Pickler
    val res = arr.map( e => e._1 + "," + e._2).mkString(",").split(",")
      .map(pickle.dumps(_)).toSeq
    new java.util.ArrayList(res)
  }

  def getColumnByName(project: String, table: String, name: String): JList[Array[Byte]] = {
    val tuple = odpsOps.getColumnByName(project, table, name)
    val pickle = new Pickler
    val res = Array(tuple._1, tuple._2).map(pickle.dumps(_)).toSeq
    new java.util.ArrayList(res)
  }

  def getColumnByIdx(project: String, table: String, idx: Int): JList[Array[Byte]] = {
    val tuple = odpsOps.getColumnByIdx(project, table, idx)
    val pickle = new Pickler
    val res = Array(tuple._1, tuple._2).map(pickle.dumps(_)).toSeq
    new java.util.ArrayList(res)
  }

  private def serialize(rdd: RDD[Array[_]], batchSize: Int): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      val pickle = new Pickler()
      if (batchSize > 1) {
        iter.grouped(batchSize).map(batched => pickle.dumps(seqAsJavaList(batched)))
      } else {
        iter.map(pickle.dumps(_))
      }
    }
  }

  private def deserialize(pyRDD: RDD[Array[Byte]], batched: Boolean): RDD[Array[_]] = {

    pyRDD.mapPartitions { iter =>
      val unpickle = new Unpickler()
      val unpickled =
        if (batched) {
          iter.flatMap { batch =>
            unpickle.loads(batch) match {
              case objs: java.util.List[_] => collectionAsScalaIterable(objs)
              case other => throw new SparkException(
                s"Unexpected type ${other.getClass.getName} for batch serialized Python RDD")
            }
          }
        } else {
          iter.map(unpickle.loads(_))
        }
      unpickled.map(_.asInstanceOf[JList[_]].toList.toArray)
    }
  }
}

object PythonOdpsAPI {
  // TODO: 可以在python端对rdd先进行一次map，把dateTime转换为python的datetime
  // 现在的python端传入的是string，读出的也是string
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
}

class PythonOdpsAPIHelper {
  
  def createPythonOdpsAPI(@transient sc: JavaSparkContext,
                          accessKeyId: String,
                          accessKeySecret: String,
                          odpsUrl: String,
                          tunnelUrl: String): PythonOdpsAPI = {
    new PythonOdpsAPI(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
  }
}
