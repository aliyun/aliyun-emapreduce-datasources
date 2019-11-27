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
import scala.util.control.NonFatal

import com.alicloud.openservices.tablestore.SyncClient
import com.alicloud.openservices.tablestore.model._
import com.alicloud.openservices.tablestore.model.StreamRecord.RecordType
import com.alicloud.openservices.tablestore.model.tunnel._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.Source

class TableStoreTestUtil extends Logging {
  private val instanceName = Option(System.getenv("OTS_INSTANCE_NAME")).getOrElse("")
  private val region = Option(System.getenv("REGION_NAME")).getOrElse("cn-hangzhou")
  private val endpoint = s"http://$instanceName.$region.ots.aliyuncs.com"
  private val tableName = "spark_test"
  private val tunnelName = "user-tunnel"
  private val accessKeyId = Option(System.getenv("ALIYUN_ACCESS_KEY_ID")).getOrElse("")
  private val accessKeySecret = Option(System.getenv("ALIYUN_ACCESS_KEY_SECRET")).getOrElse("")
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

  private[sql] def createTable(): Unit = {
    val tableMeta: TableMeta = new TableMeta(tableName)
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkString", PrimaryKeyType.STRING))
    tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PkInt", PrimaryKeyType.INTEGER))
    val tableOptions: TableOptions = new TableOptions(-1, 1)
    syncClient.createTable(new CreateTableRequest(tableMeta, tableOptions))
  }

  private[sql] def deleteTable(): Unit = {
    try {
      syncClient.deleteTable(new DeleteTableRequest(tableName))
    } catch {
      case NonFatal(ex) => // ok
    }
  }

  private[sql] def insertData(rowCount: Int): Unit = {
    var batchRequest: BatchWriteRowRequest = new BatchWriteRowRequest()
    for (i <- Range(0, rowCount)) {
      val pkBuilder: PrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
      pkBuilder.addPrimaryKeyColumn("PkString", PrimaryKeyValue.fromString(i.toString))
      pkBuilder.addPrimaryKeyColumn("PkInt", PrimaryKeyValue.fromLong(i))
      val rowPutChange: RowPutChange = new RowPutChange(tableName, pkBuilder.build())
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
    Map(
      "instance.name" -> instanceName,
      "table.name" -> tableName,
      "tunnel.id" -> origOptions.getOrElse("tunnel.id", ""),
      "endpoint" -> endpoint,
      "access.key.id" -> accessKeyId,
      "access.key.secret" -> accessKeySecret,
      "maxOffsetsPerChannel" -> origOptions.getOrElse(
        "maxOffsetsPerChannel",
        "10000"
      ),
      "catalog" -> origOptions.getOrElse("catalog", "")
    )
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
