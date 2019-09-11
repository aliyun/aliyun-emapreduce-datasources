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
