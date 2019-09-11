package org.apache.spark.sql.aliyun.tablestore

import java.nio.charset.StandardCharsets
import java.util

import com.alicloud.openservices.tablestore.model.RecordColumn.ColumnType
import com.alicloud.openservices.tablestore.model._
import org.scalatest.FunSuite

class TableStoreSourceRDDSuite extends FunSuite {
  private val testUtils = new TableStoreTestUtil()

  private val testSchema = {
    val catalog = "{\"columns\":{\"PkString\":{\"col\":\"PkString\",\"type\":\"string\"},\"PkInt\":{\"col\":\"PkInt\",\"type\":\"long\"}," +
      "\"col1\":{\"col\":\"col1\",\"type\":\"string\"}, \"col2\":{\"col\":\"col2\",\"type\":\"long\"}," +
      "\"col3\":{\"col\":\"col3\",\"type\":\"binary\"}, \"timestamp\":{\"col\":\"col4\",\"type\":\"long\"}, " +
      "\"col5\":{\"col\":\"col5\",\"type\":\"double\"}, \"col6\":{\"col\":\"col6\",\"type\":\"boolean\"}}}"
    val options =
      testUtils.getTestOptions(
        Map("catalog" -> catalog, "ots.tunnel" -> "", "maxOffsetsPerChannel" -> "10000")
      )
    TableStoreSource.tableStoreSchema(TableStoreCatalog(options).schema)
  }

  test("Extract primary key from stream record") {
    val pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    pk.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString("testString"))
    pk.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(666))
    val columns = new util.ArrayList[RecordColumn]()
    columns.add(
      new RecordColumn(new Column("col1", ColumnValue.fromString("col1")), ColumnType.PUT)
    )
    columns.add(
      new RecordColumn(new Column("col2", ColumnValue.fromString("col2")), ColumnType.PUT)
    )
    val streamRecord = testUtils.genStreamRecord(pk.build(), columns)
    assert(
      TableStoreSourceRDD.extractValue(streamRecord, "PkString", testSchema).get == "testString"
    )
    assert(TableStoreSourceRDD.extractValue(streamRecord, "PkInt", testSchema).get == 666)
    assert(TableStoreSourceRDD.extractValue(streamRecord, "unknown", testSchema).isEmpty)
  }

  test("Extract attribute column from stream record") {
    val pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    pk.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString("testString"))
    pk.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(666))
    val columns = new util.ArrayList[RecordColumn]()
    columns.add(
      new RecordColumn(new Column("col1", ColumnValue.fromString("col1")), ColumnType.PUT)
    )
    columns.add(
      new RecordColumn(new Column("col2", ColumnValue.fromLong(12345678)), ColumnType.PUT)
    )
    columns.add(
      new RecordColumn(new Column("timestamp", ColumnValue.fromString("timestamp")), ColumnType.PUT)
    )
    columns.add(
      new RecordColumn(
        new Column("col3", ColumnValue.fromBinary("col3".getBytes(StandardCharsets.UTF_8))),
        ColumnType.PUT
      )
    )
    columns.add(
      new RecordColumn(new Column("col5", ColumnValue.fromDouble(3.1415926)), ColumnType.PUT)
    )
    columns.add(
      new RecordColumn(new Column("col6", ColumnValue.fromBoolean(false)), ColumnType.PUT)
    )
    val streamRecord = testUtils.genStreamRecord(pk.build(), columns)
    assert(TableStoreSourceRDD.extractValue(streamRecord, "col1", testSchema).get == "col1")
    assert(TableStoreSourceRDD.extractValue(streamRecord, "col2", testSchema).get == 12345678)
    assert(
      new String(
        TableStoreSourceRDD
          .extractValue(streamRecord, "col3", testSchema)
          .get
          .asInstanceOf[Array[Byte]]
      ) == "col3"
    )
    assert(TableStoreSourceRDD.extractValue(streamRecord, "col5", testSchema).get == 3.1415926)
    assert(TableStoreSourceRDD.extractValue(streamRecord, "col6", testSchema).get == false)
    assert(TableStoreSourceRDD.extractValue(streamRecord, "unknown", testSchema).isEmpty)
  }

  test("Extract primary key with invalid struct type") {
    val pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
    pk.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString("testString"))
    pk.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(666))
    val columns = new util.ArrayList[RecordColumn]()
    columns.add(
      new RecordColumn(new Column("col1", ColumnValue.fromString("col1")), ColumnType.PUT)
    )
    columns.add(
      new RecordColumn(new Column("col2", ColumnValue.fromString("col2")), ColumnType.PUT)
    )
    val streamRecord = testUtils.genStreamRecord(pk.build(), columns)
    // TODO: Change testSchema
    assert(
      TableStoreSourceRDD.extractValue(streamRecord, "PkString", testSchema).get == "testString"
    )
    assert(TableStoreSourceRDD.extractValue(streamRecord, "PkInt", testSchema).get == 666)
    assert(TableStoreSourceRDD.extractValue(streamRecord, "unknown", testSchema).isEmpty)
  }
}
