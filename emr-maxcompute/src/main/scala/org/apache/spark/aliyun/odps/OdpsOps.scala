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
package org.apache.spark.aliyun.odps

import java.text.SimpleDateFormat

import scala.reflect.ClassTag

import com.aliyun.odps._
import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.account.AliyunAccount
import com.aliyun.odps.data.Record
import com.aliyun.odps.tunnel.TableTunnel
import com.aliyun.odps.tunnel.io.TunnelRecordWriter

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.function.{Function2 => JFunction2, Function3 => JFunction3}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructType

class OdpsOps(@transient sc: SparkContext, accessKeyId: String,
    accessKeySecret: String, odpsUrl: String, tunnelUrl: String)
    extends Logging with Serializable {
  @transient val account = new AliyunAccount(accessKeyId, accessKeySecret)
  @transient val odps = new Odps(account)

  odps.setEndpoint(odpsUrl)
  @transient val tunnel = new TableTunnel(odps)
  tunnel.setEndpoint(tunnelUrl)
  @transient val odpsUtils = new OdpsUtils(odps, tunnel)
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]

  /**
   * Read table from ODPS.
   * {{{
   *   OdpsOps odpsOps = ...
   *   static class RecordToLongs implements Function2<Record, TableSchema, List<Long>> {
   *     @Override
   *     public List<Long> call(Record record, TableSchema schema) throws Exception {
   *         List<Long> ret = new ArrayList<Long>();
   *         for (int i = 0; i < schema.getColumns().size(); i++) {
   *             ret.add(Long.valueOf(record.getString(i)));
   *         }
   *         return ret;
   *     }
   *    }
   *
   *    JavaRDD<List<Long>> readData = odpsOps.readTableWithJava("odps-project",
   *        "odps-table", "odps-partition", new RecordToLongs(), 2);
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job are reading.
   * @param partition The name of partition, when job is reading a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param transfer A function for transferring ODPS table to
   *        [[org.apache.spark.api.java.JavaRDD]]. We apply the function to all
   *        [[com.aliyun.odps.data.Record]] of table.
   * @param numPartition The number of RDD partition, implying the concurrency to
   *        read ODPS table.
   * @return A JavaRDD which contains all records of ODPS table.
   */
  def readTableWithJava[R](
      project: String,
      table: String,
      partition: String,
      transfer: JFunction2[Record, TableSchema, R],
      numPartition: Int): JavaRDD[R] = {
    new JavaRDD(
      readTable(project, table, partition,
        (record: Record, schema: TableSchema) => transfer.call(record, schema),
        numPartition)(fakeClassTag))(fakeClassTag)
   }

  /**
   * Read table from ODPS.
   * {{{
   *   OdpsOps odpsOps = ...
   *   static class RecordToLongs implements Function2<Record, TableSchema, List<Long>> {
   *     @Override
   *     public List<Long> call(Record record, TableSchema schema) throws Exception {
   *         List<Long> ret = new ArrayList<Long>();
   *       for (int i = 0; i < schema.getColumns().size(); i++) {
   *         ret.add(Long.valueOf(record.getString(i)));
   *       }
   *       return ret;
   *     }
   *    }
   *
   *    JavaRDD<List<Long>> readData = odpsOps.readTableWithJava("odps-project",
   *        "odps-table", new RecordToLongs(), 2);
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table from which the job is reading
   * @param transfer A function for transferring ODPS table to
   *        [[org.apache.spark.api.java.JavaRDD]]. We apply the function to all
   *        [[com.aliyun.odps.data.Record]] of table.
   * @param numPartition The number of RDD partition, implying the concurrency
   *        to read ODPS table.
   * @return A JavaRDD which contains all records of ODPS table.
   */
  def readTableWithJava[R](
      project: String,
      table: String,
      transfer: JFunction2[Record, TableSchema, R],
      numPartition: Int): JavaRDD[R] = {
    new JavaRDD(
      readTable(project, table,
        (record: Record, schema: TableSchema) => transfer.call(record, schema),
        numPartition)(fakeClassTag))(fakeClassTag)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   OdpsOps odpsOps = ...
   *   JavaRDD<List<Long>> data = ...
   *   static class SaveRecord implements Function3<List<Long>,
   *        Record, TableSchema, BoxedUnit> {
   *     @Override
   *     public BoxedUnit call(List<Long> data, Record record, TableSchema schema)
   *         throws Exception {
   *       for (int i = 0; i < schema.getColumns().size(); i++) {
   *         record.setString(i, data.get(i).toString());
   *       }
   *       return null;
   *     }
   *   }
   *
   *   odpsOps.saveToTableWithJava("odps-project", "odps-table",
   *      "odps-partition", data, new SaveRecord());
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table to which the job is writing.
   * @param partition The name of partition, when job is writing a
   *         `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param javaRdd A [[org.apache.spark.api.java.JavaRDD]] which will be
   *        written into a ODPS table.
   * @param transfer A function for transferring
   *        [[org.apache.spark.api.java.JavaRDD]] to ODPS table. We apply the
   *        function to all elements of JavaRDD.
   */
  def saveToTableWithJava[T](
      project: String,
      table: String,
      partition: String,
      javaRdd: JavaRDD[T],
      transfer: JFunction3[T, Record, TableSchema, Unit]) {
    saveToTable(project, table, partition, javaRdd.rdd,
      (t: T, record: Record, schema: TableSchema) => transfer.call(t, record,
        schema),
      false, false)(fakeClassTag)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   OdpsOps odpsOps = ...
   *   JavaRDD<List<Long>> data = ...
   *   static class SaveRecord implements Function3<List<Long>, Record,
   *       TableSchema, BoxedUnit> {
   *     @Override
   *     public BoxedUnit call(List<Long> data, Record record, TableSchema schema)
   *        throws Exception {
   *       for (int i = 0; i < schema.getColumns().size(); i++) {
   *         record.setString(i, data.get(i).toString());
   *       }
   *       return null;
   *     }
   *   }
   *
   *   odpsOps.saveToTableWithJava("odps-project", "odps-table",
   *      "odps-partition", data, new SaveRecord(), false);
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table to which the job is writing.
   * @param partition The name of partition, when job is writing a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param javaRdd A [[org.apache.spark.api.java.JavaRDD]] which will be
   *        written into a ODPS table.
   * @param transfer A function for transferring
   *        [[org.apache.spark.api.java.JavaRDD]] to ODPS table.We apply the
   *        function to all elements of JavaRDD.
   * @param defaultCreate Implying whether to create a table partition, if
   *        specific partition does not exist.
   */
  def saveToTableWithJava[T](
      project: String,
      table: String,
      partition: String,
      javaRdd: JavaRDD[T],
      transfer: JFunction3[T, Record, TableSchema, Unit],
      defaultCreate: Boolean) {
    saveToTable(project, table, partition, javaRdd.rdd,
      (t: T, record: Record, schema: TableSchema) => transfer.call(t, record, schema),
      defaultCreate, false)(fakeClassTag)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   OdpsOps odpsOps = ...
   *   JavaRDD<List<Long>> data = ...
   *   static class SaveRecord implements Function3<List<Long>, Record,
    *     TableSchema, BoxedUnit> {
   *     @Override
   *     public BoxedUnit call(List<Long> data, Record record, TableSchema schema)
    *       throws Exception {
   *       for (int i = 0; i < schema.getColumns().size(); i++) {
   *         record.setString(i, data.get(i).toString());
   *       }
   *       return null;
   *     }
   *   }
   *
   *   odpsOps.saveToTableWithJava("odps-project", "odps-table", "odps-partition",
   *      data, new SaveRecord(), false, false);
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table to which the job is writing.
   * @param partition The name of partition, when job is writing a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param javaRdd A [[org.apache.spark.api.java.JavaRDD]] which will be written
   *        into a ODPS table.
   * @param transfer A function for transferring
   *        [[org.apache.spark.api.java.JavaRDD]] to ODPS table. We apply the
   *        function to all elements of JavaRDD.
   * @param defaultCreate Implying whether to create a table partition, if the
   *        specific partition does not exist.
   * @param overwrite Implying whether to overwrite the specific partition if exists.
   *        NOTE: only support overwriting partition, not table.
   */
  def saveToTableWithJava[T](
      project: String,
      table: String,
      partition: String,
      javaRdd: JavaRDD[T],
      transfer: JFunction3[T, Record, TableSchema, Unit],
      defaultCreate: Boolean,
      overwrite: Boolean) {
    saveToTable(project, table, partition, javaRdd.rdd,
      (t: T, record: Record, schema: TableSchema) => transfer.call(t, record,
        schema),
      defaultCreate, overwrite)(fakeClassTag)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   OdpsOps odpsOps = ...
   *   JavaRDD<List<Long>> data = ...
   *   static class SaveRecord implements Function3<List<Long>, Record,
   *      TableSchema, BoxedUnit> {
   *     @Override
   *     public BoxedUnit call(List<Long> data, Record record, TableSchema schema)
   *        throws Exception {
   *       for (int i = 0; i < schema.getColumns().size(); i++) {
   *         record.setString(i, data.get(i).toString());
   *       }
   *       return null;
   *     }
   *   }
   *
   *   odpsOps.saveToTableWithJava("odps-project", "odps-table", data,
   *      new SaveRecord());
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table to which the job is writing.
   * @param javaRdd A [[org.apache.spark.api.java.JavaRDD]] which will be
   *        written into a ODPS table.
   * @param transfer A function for transferring
   *        [[org.apache.spark.api.java.JavaRDD]] to ODPS table. We apply the
   *        function to all elements of JavaRDD.
   */
  def saveToTableWithJava[T](
      project: String,
      table: String,
      javaRdd: JavaRDD[T],
      transfer: JFunction3[T, Record, TableSchema, Unit]) {
    saveToTable(project, table, javaRdd.rdd,
      (t: T, record: Record, schema: TableSchema) =>
        transfer.call(t, record, schema))(fakeClassTag)
  }

  /**
   * Read table from ODPS.
   * {{{
   *   val odpsOps = ...
   *   val odpsTable = odpsOps.readTable("odps-project", "odps-table",
   *      "odps-partition", readFunc, 2)
   *
   *   def readFunc(record: Record, schema: TableSchema): Array[Long] = {
   *     val ret = new Array[Long](schema.getColumns.size())
   *     for (i <- 0 until schema.getColumns.size()) {
   *       ret(i) = record.getString(i).toLong
   *     }
   *     ret
   *   }
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job is reading.
   * @param partition The name of partition, when job is reading a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param transfer A function for transferring ODPS table to
   *        [[org.apache.spark.rdd.RDD]]. We apply the function to all
   *        [[com.aliyun.odps.data.Record]] of table.
   * @param numPartition The number of RDD partition, implying the concurrency
   *        to read ODPS table.
   * @return A RDD which contains all records of ODPS table.
   */
  @unchecked
  def readTable[T: ClassTag](
      project: String,
      table: String,
      partition: String,
      transfer: (Record, TableSchema) => T,
      numPartition: Int): RDD[T] = {
    val func = sc.clean(transfer)
    if(!partition.equals("all")) {
      new OdpsRDD[T](sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl,
        project, table, partition, numPartition, func)
    } else {
      odpsUtils.getAllPartitionSpecs(project, table).map(ptSpec => {
        new OdpsRDD[T](sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl,
          project, table, ptSpec.toString, numPartition, func)
      }).map(_.asInstanceOf[RDD[T]]).reduce((r1, r2) => r1.union(r2))
    }
  }

  /**
   * Read table from ODPS.
   * {{{
   *   val odpsOps = ...
   *   val odpsTable = odpsOps.readTable("odps-project", "odps-table", readFunc, 2)
   *
   *   def readFunc(record: Record, schema: TableSchema): Array[Long] = {
   *     val ret = new Array[Long](schema.getColumns.size())
   *     for (i <- 0 until schema.getColumns.size()) {
   *       ret(i) = record.getString(i).toLong
   *     }
   *     ret
   *   }
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job is reading.
   * @param transfer A function for transferring ODPS table to
   *        [[org.apache.spark.rdd.RDD]]. We apply the function to all
   *        [[com.aliyun.odps.data.Record]] of table.
   * @param numPartition The number of RDD partition, implying the concurrency
   *        to read ODPS table.
   * @return A RDD which contains all records of ODPS table.
   */
  @unchecked
  def readTable[T: ClassTag](
      project: String,
      table: String,
      transfer: (Record, TableSchema) => T,
      numPartition: Int): RDD[T] = {
    val func = sc.clean(transfer)
    new OdpsRDD[T](sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl,
      project, table, numPartition, func)
  }

  /**
   * Load ODPS table into [[org.apache.spark.sql.DataFrame]].
   * {{{
   *   val sqlContext = ...
   *   val odpsOps = ...
   *   val odpstableDF = odpsOps.loadOdpsTable(sqlContext, "odps-project",
   *     "odps-table", "odps-partition", Array(0, 2, 3), 2)
   * }}}
   * @param sqlContext A Spark SQL context
   * @param project The name of ODPS project.
   * @param table The name of table, which job is reading.
   * @param partition The name of partition, when job is reading a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param cols Implying to load which columns
   * @param numPartition The number of RDD partition, implying the concurrency
   *        to read ODPS table.
   * @return A DataFrame which contains relevant records of ODPS table.
   */
  def loadOdpsTable(
      sqlContext: SQLContext,
      project: String,
      table: String,
      partition: String,
      cols: Array[Int],
      numPartition: Int): DataFrame = {
    val tableSchema = odpsUtils.getTableSchema(project, table, false)
    val colsLen = tableSchema.length
    val schema = prepareSchema(cols, tableSchema, project, table, false)
    val columns = prepareCols(cols, colsLen)
    val rdd = readTable(project, table, partition, readTransfer(columns),
      numPartition).map(e => { Row.fromSeq(e.toSeq) })

    sqlContext.createDataFrame(rdd, schema)
  }

  /**
   * Load ODPS table into [[org.apache.spark.sql.DataFrame]].
   * {{{
   *   val sqlContext = ...
   *   val odpsOps = ...
   *   val odpstableDF = odpsOps.loadOdpsTable(sqlContext, "odps-project",
   *     "odps-table", Array(0, 2, 3), 2)
   * }}}
   * @param sqlContext A Spark SQL context
   * @param project The name of ODPS project.
   * @param table The name of table, which job is reading.
   * @param cols Implying to load which columns, i.e. Array(0, 1, 3).
   * @param numPartition The number of RDD partition, implying the concurrency
   *        to read ODPS table.
   * @return A DataFrame which contains relevant records of ODPS table.
   */
  def loadOdpsTable(
      sqlContext: SQLContext,
      project: String,
      table: String,
      cols: Array[Int],
      numPartition: Int): DataFrame = {
    val tableSchema = odpsUtils.getTableSchema(project, table, false)
    val colsLen = tableSchema.length
    val schema = prepareSchema(cols, tableSchema, project, table, false)
    val cols_ = prepareCols(cols, colsLen)
    val rdd = readTable(project, table, readTransfer(cols_), numPartition)
        .map(e => { Row.fromSeq(e.toSeq) })

    sqlContext.createDataFrame(rdd, schema)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   val odpsOps = ...
   *   val data: RDD[Array[Long]] = ...
   *   odps.saveToTable("odps-project", "odps-table", "odps-partition", data,
   *     writeFunc)
   *
   *   def writeFunc(kv: Array[Long], record: Record, schema: TableSchema) {
   *     for (i <- 0 until schema.getColumns.size()) {
   *       record.setString(i, kv(i).toString)
   *     }
   *   }
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job is writing.
   * @param partition The name of partition, when job is writing a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param rdd A [[org.apache.spark.rdd.RDD]] which will be written into a
   *        ODPS table.
   * @param transfer A function for transferring [[org.apache.spark.rdd.RDD]]
   *        to ODPS table. We apply the function to all elements of RDD.
   */
  def saveToTable[T: ClassTag](
      project: String,
      table: String,
      partition: String,
      rdd: RDD[T],
      transfer: (T, Record, TableSchema) => Unit) {
    saveToTable(project, table, partition, rdd, transfer, false, false)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   val odpsOps = ...
   *   val data: RDD[Array[Long]] = ...
   *   odps.saveToTable("odps-project", "odps-table", "odps-partition", data,
   *     writeFunc, false)
   *
   *   def writeFunc(kv: Array[Long], record: Record, schema: TableSchema) {
   *     for (i <- 0 until schema.getColumns.size()) {
   *       record.setString(i, kv(i).toString)
   *     }
   *   }
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job is writing.
   * @param partition The name of partition, when job is writing a
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param rdd A [[org.apache.spark.rdd.RDD]] which will be written into a
   *        ODPS table.
   * @param transfer A function for transferring [[org.apache.spark.rdd.RDD]]
   *        to ODPS table. We apply the function to all elements of RDD.
   * @param defaultCreate Implying whether to create a table partition, if the
   *        specific partition does not exist.
   */
  def saveToTable[T: ClassTag](
      project: String,
      table: String,
      partition: String,
      rdd: RDD[T],
      transfer: (T, Record, TableSchema) => Unit,
      defaultCreate: Boolean) {
    saveToTable(project, table, partition, rdd, transfer, defaultCreate, false)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   val odpsOps = ...
   *   val data: RDD[Array[Long]] = ...
   *   odps.saveToTable("odps-project", "odps-table", "odps-partition", data,
   *     writeFunc, false, false)
   *
   *   def writeFunc(kv: Array[Long], record: Record, schema: TableSchema) {
   *     for (i <- 0 until schema.getColumns.size()) {
   *       record.setString(i, kv(i).toString)
   *     }
   *   }
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job is writing.
   * @param partition The name of partition, when job is writing a Partitioned
   *        `Partitioned Table`, like pt='xxx',ds='yyy'.
   * @param rdd A org.apache.spark.rdd.RDD which will be written into a ODPS table.
   * @param transfer A function for transferring org.apache.spark.rdd.RDD to
   *        ODPS table. We apply the function to all elements of RDD.
   * @param defaultCreate Implying whether to create a table partition, if the
   *        specific partition does not exist.
   * @param overwrite Implying whether to overwrite the specific partition if
   *        exists. NOTE: only support overwriting partition, not table.
   */
  @unchecked
  def saveToTable[T: ClassTag](
      project: String,
      table: String,
      partition: String,
      rdd: RDD[T],
      transfer: (T, Record, TableSchema) => Unit,
      defaultCreate: Boolean,
      overwrite: Boolean) {

    def transfer0(t: T, record: Record, scheme: TableSchema): Record = {
      transfer(t, record, scheme)
      record
    }

    val func = sc.clean(transfer0 _)
    odps.setDefaultProject(project)
    val partitionSpec = new PartitionSpec(partition)

    if(overwrite) {
      val success = dropPartition(project, table, partition)
      if(!success) {
        logInfo("delete partition failed.")
      }
      createPartition(project, table, partition)
    }

    if(defaultCreate) {
      createPartition(project, table, partition)
    }

    val uploadSession = tunnel.createUploadSession(project, table, partitionSpec)
    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val account_ = new AliyunAccount(accessKeyId, accessKeySecret)
      val odps_ = new Odps(account_)
      odps_.setDefaultProject(project)
      odps_.setEndpoint(odpsUrl)
      val tunnel_ = new TableTunnel(odps_)
      tunnel_.setEndpoint(tunnelUrl)
      val partitionSpec_ = new PartitionSpec(partition)
      val uploadSession_ = tunnel_.getUploadSession(project, table,
        partitionSpec_, uploadId)
      val writer = uploadSession_.openRecordWriter(context.partitionId)

      // for odps metrics monitor
      var recordsWritten = 0L
      val outputMetrics = context.taskMetrics.outputMetrics

      while (iter.hasNext) {
        val value = iter.next()
        logDebug("context id: " + context.partitionId + " write: " + value)
        writer.write(func(value, uploadSession_.newRecord(),
          uploadSession_.getSchema))
        recordsWritten += 1
      }
      logDebug("ready context id: " + context.partitionId)
      writer.close()
      logDebug("finish context id: " + context.partitionId)
      outputMetrics.setRecordsWritten(recordsWritten)
      val totalBytes = writer.asInstanceOf[TunnelRecordWriter].getTotalBytes
      outputMetrics.setBytesWritten(totalBytes)
    }

    sc.runJob(rdd, writeToFile _)
    val arr = Array.tabulate(rdd.partitions.length)(l => Long.box(l))
    uploadSession.commit(arr)
  }

  /**
   * Save a RDD to ODPS table.
   * {{{
   *   val odpsOps = ...
   *   val data: RDD[Array[Long]] = ...
   *   odps.saveToTable("odps-project", "odps-table", data, writeFunc)
   *
   *   def writeFunc(kv: Array[Long], record: Record, schema: TableSchema) {
   *     for (i <- 0 until schema.getColumns.size()) {
   *       record.setString(i, kv(i).toString)
   *     }
   *   }
   * }}}
   * @param project The name of ODPS project.
   * @param table The name of table, which job is writing.
   * @param rdd A org.apache.spark.rdd.RDD which will be written into a ODPS table.
   * @param transfer A function for transferring org.apache.spark.rdd.RDD to
   *                 ODPS table. We apply the function to all elements of RDD.
   */
  @unchecked
  def saveToTable[T: ClassTag](
      project: String,
      table: String,
      rdd: RDD[T],
      transfer: (T, Record, TableSchema) => Unit) {

    def transfer0(t: T, record: Record, scheme: TableSchema): Record = {
      transfer(t, record, scheme)
      record
    }

    val func = sc.clean(transfer0 _)
    odps.setDefaultProject(project)
    val uploadSession = tunnel.createUploadSession(project, table)
    logInfo("Odps upload session status is: " + uploadSession.getStatus.toString)
    val uploadId = uploadSession.getId

    def writeToFile(context: TaskContext, iter: Iterator[T]) {
      val account_ = new AliyunAccount(accessKeyId, accessKeySecret)
      val odps_ = new Odps(account_)
      odps_.setDefaultProject(project)
      odps_.setEndpoint(odpsUrl)
      val tunnel_ = new TableTunnel(odps_)
      tunnel_.setEndpoint(tunnelUrl)
      val uploadSession_ = tunnel_.getUploadSession(project, table, uploadId)
      val writer = uploadSession_.openRecordWriter(context.partitionId)

      // for odps metrics monitor
      var recordsWritten = 0L
      val outputMetrics = context.taskMetrics.outputMetrics

      while (iter.hasNext) {
        val value = iter.next()
        logDebug("context id: " + context.partitionId + " write: " + value)
        writer.write(func(value, uploadSession_.newRecord(),
          uploadSession_.getSchema))
        recordsWritten += 1
      }
      logDebug("ready context id: " + context.partitionId)
      writer.close()
      logDebug("finish context id: " + context.partitionId)
      outputMetrics.setRecordsWritten(recordsWritten)
      val totalBytes = writer.asInstanceOf[TunnelRecordWriter].getTotalBytes
      outputMetrics.setBytesWritten(totalBytes)
    }

    sc.runJob(rdd, writeToFile _)
    val arr = Array.tabulate(rdd.partitions.length)(l => Long.box(l))
    uploadSession.commit(arr)
  }

  private def prepareCols(cols: Array[Int], columnsLen: Int): Array[Int] = {
    if (cols.length == 0) {
      Array.range(0, columnsLen)
    } else {
      cols
    }
  }

  private def prepareSchema(
      cols: Array[Int],
      tableSchema: Array[(String, TypeInfo)],
      project: String,
      table: String,
      isPartition: Boolean): StructType = {
    val tableSchema = odpsUtils.getTableSchema(project, table, isPartition)
    val columns = if (cols.length == 0) {
      Array.range(0, tableSchema.length)
    } else {
      cols
    }.sorted

    StructType(
      columns.map(idx => {
        OdpsUtils.getCatalystType(tableSchema(idx)._1, tableSchema(idx)._2, true)
      })
    )
  }

  private def readTransfer(cols: Array[Int])(record: Record, schema: TableSchema):
      Array[_] = {
    cols.sorted.map { idx =>
      val col = schema.getColumn(idx)
      try {
        OdpsUtils.odpsData2SparkData(col.getTypeInfo, false)(record.get(idx))
      } catch {
        case e: Exception =>
          log.error(s"Can not transfer record column value, idx: $idx, " +
            s"type: ${col.getTypeInfo.getOdpsType}, value ${record.toArray.apply(idx)}")
          throw e
      }
    }
  }

  def getTableSchema(project: String, table: String, isPartition: Boolean):
      Array[(String, TypeInfo)] = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val columns = if (isPartition) schema.getPartitionColumns else schema.getColumns
    columns.toArray(new Array[Column](0)).map(e => (e.getName, e.getTypeInfo))
  }

  def getColumnByName(project: String, table: String, name: String):
      (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val idx = schema.getColumnIndex(name)
    val colType = schema.getColumn(name).getTypeInfo
    val field = OdpsUtils.getCatalystType(name, colType, true)

    (idx.toString, field.dataType.simpleString)
  }

  def getColumnByIdx(project: String, table: String, idx: Int):
      (String, String) = {
    odps.setDefaultProject(project)
    val schema = odps.tables().get(table).getSchema
    val column = schema.getColumn(idx)
    val name = column.getName
    val colType = schema.getColumn(name).getTypeInfo
    val field = OdpsUtils.getCatalystType(name, colType, true)

    (name, field.dataType.simpleString)
  }

  private def checkTableAndPartition(
      project: String,
      table: String,
      pname: String): (Boolean, Boolean) = {
    val partitionSpec_ = new PartitionSpec(pname)
    odps.setDefaultProject(project)
    val tables = odps.tables()
    val tableExist = tables.exists(table)
    if(!tableExist) {
      logWarning("table " + table + " do not exist!")
      return (false, false)
    }

    val partitions = tables.get(table).getPartitions
    val partitionFilter = partitions.toArray(new Array[Partition](0)).iterator
      .map(e => e.getPartitionSpec)
      .filter(f => f.toString.equals(partitionSpec_.toString))
    val partitionExist = if (partitionFilter.size == 0) false else true
    if(partitionExist) {
      (true, true)
    } else {
      (true, false)
    }
  }

  private def dropPartition(
      project: String,
      table: String,
      pname: String): Boolean = {
    try {
      val (_, partitionE) = checkTableAndPartition(project, table, pname)
      if(!partitionE) {
        return true
      }
      odps.setDefaultProject(project)
      val partitionSpec = new PartitionSpec(pname)
      odps.tables().get(table).deletePartition(partitionSpec)
      true
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when delete partition " +
            pname + " of " + table + ".")
        logError(e.getMessage)
        false
    }
  }

  private def dropTable(
      project: String,
      table: String): Boolean = {
    try {
      val (tableE, _) = checkTableAndPartition(project, table, "random")
      if(!tableE) {
        return true
      }
      odps.setDefaultProject(project)
      odps.tables().delete(table)
      true
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when delete table " + table + ".")
        logError(e.getMessage)
        false
    }
  }

  private def createPartition(
      project: String,
      table: String,
      pname: String): Boolean = {
    val partitionSpec_ = new PartitionSpec(pname)
    val (tableE, partitionE) = checkTableAndPartition(project, table, pname)
    if(!tableE) {
      logWarning("table " + table + " do not exist, FAILED.")
      return false
    } else if (partitionE) {
      logWarning("table " + table + " partition " + pname +
          " exist, no need to create.")
      return true
    }

    try {
      odps.tables().get(table).createPartition(partitionSpec_)
    } catch {
      case e: OdpsException =>
        logError("somethings wrong happens when create table " + table +
            " partition " + pname + ".")
        return false
    }

    true
  }
}

object OdpsOps {
  def apply(
      sc: SparkContext,
      accessKeyId: String,
      accessKeySecret: String,
      odpsUrl: String,
      tunnelUrl: String): OdpsOps = {
    new OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
  }
}
