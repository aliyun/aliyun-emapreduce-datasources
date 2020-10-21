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

import java.nio.charset.StandardCharsets
import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.alicloud.openservices.tablestore.SyncClient
import com.alicloud.openservices.tablestore.model._
import com.alicloud.openservices.tablestore.model.StreamRecord.RecordType
import com.alicloud.openservices.tablestore.model.search._
import com.alicloud.openservices.tablestore.model.tunnel._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType


class TableStoreTestUtil extends Logging {
  private val instanceName = Option(System.getenv("OTS_INSTANCE_NAME")).getOrElse("")
  private val region = Option(System.getenv("REGION_NAME")).getOrElse("cn-hangzhou")
  private val endpoint = s"http://$instanceName.$region.ots.aliyuncs.com"
  private val tableName = "spark_test2"
  private val tunnelName = "user-tunnel"
  private val accessKeyId = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
  private val accessKeySecret = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")
  private val pushdownRangeLong = Option(System.getenv("PUSHDOWN_RANGE_LONG")).getOrElse("true")
  private val pushdownRangeString = Option(System.getenv("PUSHDOWN_RANGE_STRING")).getOrElse("true")
  private lazy val tunnelClient = TableStoreOffsetReader.getOrCreateTunnelClient(
    endpoint,
    accessKeyId,
    accessKeySecret,
    instanceName
  )
  private lazy val syncClient = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)

  private lazy val _spark = SparkSession.builder
    .appName("TableStoreTestUtils")
    .master("local[5]")
    .getOrCreate()

  private[sql] def createSearchIndex(tableName: String, indexName: String,
                                     indexSchema: IndexSchema): Unit = {
    val searchIndexRequest = new CreateSearchIndexRequest()
    if (indexSchema==null) {
      val indexSchema = new IndexSchema()
      val fieldSchemaList = new util.ArrayList[FieldSchema]()
      fieldSchemaList.add(new FieldSchema("PkString", FieldType.KEYWORD))
      fieldSchemaList.add(new FieldSchema("PkInt", FieldType.LONG))
      fieldSchemaList.add(new FieldSchema("col1", FieldType.KEYWORD))
      fieldSchemaList.add(new FieldSchema("col2", FieldType.LONG))
      fieldSchemaList.add(new FieldSchema("col3", FieldType.BOOLEAN))
      fieldSchemaList.add(new FieldSchema("col4", FieldType.LONG))
      fieldSchemaList.add(new FieldSchema("col5", FieldType.DOUBLE))
      fieldSchemaList.add(new FieldSchema("col6", FieldType.BOOLEAN))
      indexSchema.setFieldSchemas(fieldSchemaList)
      searchIndexRequest.setIndexSchema(indexSchema)
    } else {
      searchIndexRequest.setIndexSchema(indexSchema)
    }
    searchIndexRequest.setTableName(tableName)
    searchIndexRequest.setIndexName(indexName)
    syncClient.createSearchIndex(searchIndexRequest)
  }

  private[sql] def createTunnel(tunnelType: TunnelType): String = {
    val createResp = tunnelClient.createTunnel(
      new CreateTunnelRequest(tableName, tunnelName, tunnelType)
    )
    createResp.getTunnelId
  }

  private[sql] def checkTunnelReady(tunnelId: String, tunnelStage: TunnelStage): Boolean = {
    val describeResp = tunnelClient.describeTunnel(new DescribeTunnelRequest(tableName, tunnelName))
    var isReady: Boolean = false
    describeResp.getChannelInfos.asScala.foreach { channelInfo =>
      if (channelInfo.getChannelStatus == ChannelStatus.OPEN) {
        if (tunnelStage == TunnelStage.ProcessBaseData &&
          channelInfo.getChannelType == ChannelType.BaseData) {
          isReady = true
        }
        if (tunnelStage == TunnelStage.ProcessStream &&
          channelInfo.getChannelType == ChannelType.Stream) {
          isReady = true
        }
      }
    }
    isReady
  }

  private[sql] def deleteTunnel(): Unit = {
    try {
      tunnelClient.deleteTunnel(new DeleteTunnelRequest(tableName, tunnelName))
    } catch {
      case NonFatal(ex) => // ok
    }
  }

  private[sql] def containSearchIndex(searchIndexName: String, tableName: String): Boolean = {
    val tableList: java.util.List[SearchIndexInfo] = listSearchIndex(tableName)
    import collection.JavaConverters._
    for (temp <- tableList.asScala) {
      if (temp.getIndexName.equals(searchIndexName)) {
        return true
      }
    }
    false
  }

  private[sql] def containTable(tableName: String): Boolean = {
    val tableList: java.util.List[String] = listTable()
    import collection.JavaConverters._
    tableList.asScala.contains(tableName)
  }


  private[sql] def createTable(): Unit = {
    val tableMeta: TableMeta = new TableMeta(tableName)
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkString", PrimaryKeyType.STRING))
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkInt", PrimaryKeyType.INTEGER))
    val tableOptions: TableOptions = new TableOptions(-1, 1)
    syncClient.createTable(new CreateTableRequest(tableMeta, tableOptions))
  }

  private[sql] def createTable(tableName: String): Unit = {
    val tableMeta: TableMeta = new TableMeta(tableName)
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkString", PrimaryKeyType.STRING))
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkInt", PrimaryKeyType.INTEGER))
    val tableOptions: TableOptions = new TableOptions(-1, 1)
    syncClient.createTable(new CreateTableRequest(tableMeta, tableOptions))
  }

  private[sql] def createTable(tblName: String = tableName, maxVersion: Int = 1,
                               definedColumns: Array[DefinedColumnSchema] = Array()): Unit = {
    val tableMeta: TableMeta = new TableMeta(tblName)
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkString", PrimaryKeyType.STRING))
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkInt", PrimaryKeyType.INTEGER))
    if (definedColumns != null && !definedColumns.isEmpty) {
      tableMeta.addDefinedColumns(definedColumns)
    }
    val tableOptions: TableOptions = new TableOptions(-1, maxVersion)
    syncClient.createTable(new CreateTableRequest(tableMeta, tableOptions))
  }

  private[sql] def createIndex(tblName: String, indexName: String): Unit = {
    val indexMeta = new IndexMeta(indexName)
    indexMeta.addPrimaryKeyColumn("PkString")
    indexMeta.addPrimaryKeyColumn("PkInt")
    indexMeta.addDefinedColumn("col1")
    indexMeta.addDefinedColumn("timestamp")

    syncClient.createIndex(new CreateIndexRequest(tblName, indexMeta, false))
  }

  private[sql] def deleteTable(): Unit = {
    try {
      syncClient.deleteTable(new DeleteTableRequest(tableName))
    } catch {
      case NonFatal(ex) => // ok
    }
  }

  private[sql] def deleteTable(tblName: String = tableName): Unit = {
    try {
      syncClient.deleteTable(new DeleteTableRequest(tblName))
    } catch {
      case NonFatal(ex) => // ok
    }
  }

  private[sql] def listTable(): java.util.List[String] = {
    val response = syncClient.listTable()
    response.getTableNames
  }

  private[sql] def listSearchIndex(tableName: String): java.util.List[SearchIndexInfo] = {
    val listSearchIndexRequest = new ListSearchIndexRequest()
    listSearchIndexRequest.setTableName(tableName)
    val response = syncClient.listSearchIndex(listSearchIndexRequest)
    response.getIndexInfos
  }

  private[sql] def insertData(rowCount: Int, tblName: String = tableName): Unit = {
    var batchRequest: BatchWriteRowRequest = new BatchWriteRowRequest()
    for (i <- Range(0, rowCount)) {
      val pkBuilder: PrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
      pkBuilder.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString(i.toString))
      pkBuilder.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(i))
      val rowPutChange: RowPutChange = new RowPutChange(tblName, pkBuilder.build())
      rowPutChange.addColumn("col1", ColumnValue.fromString(i.toString))
      rowPutChange.addColumn("col2", ColumnValue.fromLong(i))
      rowPutChange.addColumn(
        "col3",
        ColumnValue.fromBinary(i.toString.getBytes(StandardCharsets.UTF_8))
      )
      rowPutChange.addColumn("timestamp", ColumnValue.fromLong(12345678 + i))
      rowPutChange.addColumn("col5", ColumnValue.fromDouble(3.1415926 + i))
      rowPutChange.addColumn("col6", ColumnValue.fromBoolean(i % 2 == 0))
      batchRequest.addRowChange(rowPutChange)
      if (batchRequest.getRowsCount == 200) {
        syncClient.batchWriteRow(batchRequest)
        batchRequest = new BatchWriteRowRequest()
      }
    }
    // Write the last batch
    if (batchRequest.getRowsCount > 0) {
      syncClient.batchWriteRow(batchRequest)
    }
  }

  private[sql] def getRow(tblName: String, primaryKey: PrimaryKey, maxVersion: Int = 1): Row = {
    val criteria = new SingleRowQueryCriteria(tblName, primaryKey)
    criteria.setMaxVersions(maxVersion)
    val getRowResponse = syncClient.getRow(new GetRowRequest(criteria))
    getRowResponse.getRow
  }

  private[sql] def getCount(tblName: String, primaryKeys: Array[PrimaryKey]): Int = {
    val multiRowQueryCriteria = new MultiRowQueryCriteria(tblName)
    primaryKeys.foreach(multiRowQueryCriteria.addRow(_))
    multiRowQueryCriteria.setMaxVersions(1)

    val batchGetRowRequest = new BatchGetRowRequest()
    batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria)
    val batchGetRow = syncClient.batchGetRow(batchGetRowRequest)
    batchGetRow.getBatchGetRowResult(tblName).size()
  }

  private[sql] def createTestSourceDataFrame(options: Map[String, String]): DataFrame = {
    val tunnelId =
      options.getOrElse("tunnel.id", "d4db52c8-4a87-4051-956e-8eeb171a1fce")
    val maxOffsetsPerChannel =
      options.getOrElse("maxOffsetsPerChannel", 10000 + "")
    val catalog = options.getOrElse("catalog", "")

    _spark.readStream
      .format("tablestore")
      .option("instance.name", instanceName)
      .option("table.name", tableName)
      .option("tunnel.id", tunnelId)
      .option("endpoint", endpoint)
      .option("access.key.id", accessKeyId)
      .option("access.key.secret", accessKeySecret)
      .option("maxOffsetsPerChannel", maxOffsetsPerChannel)
      .option("catalog", catalog)
      .load()
  }

  private[sql] def getTestOptions(origOptions: Map[String, String]): Map[String, String] = {
    val baseMap = mutable.Map(
      "instance.name" -> instanceName,
      "table.name" -> tableName,
      "endpoint" -> endpoint,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "search.index.name" -> "",
      "push.down.range.long" -> pushdownRangeLong,
      "push.down.range.string" -> pushdownRangeString,
      "maxOffsetsPerChannel" -> origOptions.getOrElse(
        "maxOffsetsPerChannel",
        "10000"
      ),
      "catalog" -> origOptions.getOrElse("catalog", "")
    )
    if (origOptions.contains("tunnel.id")) {
      baseMap("tunnel.id") = origOptions("tunnel.id")
    }
    baseMap.toMap
  }

  private[sql] def getTestOptions(origOptions: Map[String, String],
                                  tableName: String,
                                  searchIndexName: String ): Map[String, String] = {
    val baseMap = mutable.Map(
      "instance.name" -> instanceName,
      "table.name" -> tableName,
      "endpoint" -> endpoint,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "search.index.name" -> searchIndexName,
      "push.down.range.long" -> pushdownRangeLong,
      "push.down.range.string" -> pushdownRangeString,
      "maxOffsetsPerChannel" -> origOptions.getOrElse(
        "maxOffsetsPerChannel",
        "10000"
      ),
      "catalog" -> origOptions.getOrElse("catalog", "")
    )
    if (origOptions.contains("tunnel.id")) {
      baseMap("tunnel.id") = origOptions("tunnel.id")
    }
    baseMap.toMap
  }

  private[sql] def getTestOptions(origOptions: Map[String, String],
                                  tableName: String, searchIndexName: String,
                                  pushdownRangeLong: String,
                                  pushdownRangeString: String
                                 ): Map[String, String] = {
    val baseMap = mutable.Map(
      "instance.name" -> instanceName,
      "table.name" -> tableName,
      "endpoint" -> endpoint,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "search.index.name" -> searchIndexName,
      "push.down.range.long" -> pushdownRangeLong,
      "push.down.range.string" -> pushdownRangeString,
      "maxOffsetsPerChannel" -> origOptions.getOrElse(
        "maxOffsetsPerChannel",
        "10000"
      ),
      "catalog" -> origOptions.getOrElse("catalog", "")
      )
      if (origOptions.contains("tunnel.id")) {
        baseMap("tunnel.id") = origOptions("tunnel.id")
    }
    baseMap.toMap
  }

  private[sql] def getUnionOptions(origOptions: Map[String, String]): Map[String, String] = {
    val baseMap = mutable.Map(
      "instance.name" -> instanceName,
      "endpoint" -> endpoint,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret
    )
    origOptions.foreach(org => baseMap.put(org._1, org._2))
    baseMap.toMap
  }

  private[sql] def createTestSource(
      sqlContext: SQLContext,
      options: Map[String, String]): Source = {
    val metaDataPath = "/tmp/temporary-" + UUID.randomUUID.toString
    val schema = TableStoreCatalog(options).schema
    val provider = new TableStoreSourceProvider()
    val fullOptions = getTestOptions(options)
    provider.createSource(
      sqlContext,
      metaDataPath,
      Some(schema),
      provider.shortName(),
      fullOptions
    )
  }

  private[sql] def createTestRelation(
      sqlContext: SQLContext,
      options: Map[String, String]): BaseRelation = {
    val provider = new TableStoreSourceProvider()
    val fullOptions = getTestOptions(options)
    provider.createRelation(sqlContext, fullOptions)
  }

  private[sql] def createTestStructType(): StructType = {
    TableStoreCatalog(Map("catalog" -> TableStoreTestUtil.catalog)).schema
  }

  private[sql] def genStreamRecord(
      pk: PrimaryKey,
      columns: util.List[RecordColumn]): StreamRecord = {
    val record = new StreamRecord()
    record.setRecordType(RecordType.PUT)
    record.setPrimaryKey(pk)
    record.setColumns(columns)
    record
  }
}

object TableStoreTestUtil {
  val catalog: String =
    """
      |{
      |  "columns":{
      |    "PkString":{
      |      "col":"PkString",
      |      "type":"string"
      |    },
      |    "PkInt":{
      |      "col":"PkInt",
      |      "type":"long"
      |    },
      |    "col1":{
      |      "col":"col1",
      |      "type":"string"
      |    },
      |    "col2":{
      |      "col":"col2",
      |      "type":"long"
      |    },
      |    "col3":{
      |      "col":"col3",
      |      "type":"binary"
      |    },
      |    "timestamp":{
      |      "col":"col4",
      |      "type":"long"
      |    },
      |    "col5":{
      |      "col":"col5",
      |      "type":"double"
      |    },
      |    "col6":{
      |      "col":"col6",
      |      "type":"boolean"
      |    }
      |  }
      |}
    """.stripMargin
}
