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

import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}

case class TableStoreCatalog(schema: StructType)

object TableStoreCatalog {
  val tableCatalog = "catalog"
  val columns = "columns"
  val col = "col"
  val `type` = "type"

  def apply(parameters: Map[String, String]): TableStoreCatalog = {
    val jString = parameters(tableCatalog)
    val jObj = parse(jString).asInstanceOf[JObject]
    val schema = StructType(
      getColsPreservingOrder(jObj).map(e =>
        StructField(e._1, CatalystSqlParser.parseDataType(e._2(`type`)))))
    new TableStoreCatalog(schema)
  }

  def getColsPreservingOrder(jObj: JObject): Seq[(String, Map[String, String])] = {
    val jCols = jObj.obj.find(_._1 == columns).get._2.asInstanceOf[JObject]
    jCols.obj.map { case (name, jvalue) =>
      (name, jvalue.values.asInstanceOf[Map[String, String]])
    }
  }
}
