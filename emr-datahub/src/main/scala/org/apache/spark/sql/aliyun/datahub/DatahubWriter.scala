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

package org.apache.spark.sql.aliyun.datahub

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class DatahubWriter(
    project: Option[String],
    topic: Option[String],
    datahubOptions: Map[String, String],
    schema: Option[StructType]) extends DataSourceWriter {

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def createWriterFactory(): DatahubWriterFactory = {
    DatahubWriterFactory(project, topic, datahubOptions, schema)
  }
}

case class DatahubWriterFactory(
    project: Option[String],
    topic: Option[String],
    datahubParams: Map[String, String],
    schema: Option[StructType]) extends DataWriterFactory[InternalRow] {

  override def createDataWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new DatahubDataWriter(project, topic, datahubParams, schema)
  }
}
