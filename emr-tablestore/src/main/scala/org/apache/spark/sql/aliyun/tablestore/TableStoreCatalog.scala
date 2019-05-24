package org.apache.spark.sql.aliyun.tablestore

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
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
      getColsPreservingOrder(jObj).map(e => StructField(e._1, CatalystSqlParser.parseDataType(e._2(`type`)))))
    new TableStoreCatalog(schema)
  }

  def getColsPreservingOrder(jObj: JObject): Seq[(String, Map[String, String])] = {
    val jCols = jObj.obj.find(_._1 == columns).get._2.asInstanceOf[JObject]
    jCols.obj.map { case (name, jvalue) =>
      (name, jvalue.values.asInstanceOf[Map[String, String]])
    }
  }
}