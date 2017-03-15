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
package org.apache.spark.aliyun.odps.datasource

class ODPSOptions(
  @transient private val parameters: Map[String, String])
  extends Serializable {

  // Aliyun Account accessKeySecret
  val accessKeySecret =
    parameters.getOrElse("accessKeySecret", sys.error("Option 'accessKeySecret' not specified"))

  // Aliyun Account accessKeyId
  val accessKeyId =
    parameters.getOrElse("accessKeyId", sys.error("Option 'accessKeyId' not specified"))

  // the odps endpoint URL
  val odpsUrl = parameters.getOrElse("odpsUrl", sys.error("Option 'odpsUrl' not specified"))

  // the TableTunnel endpoint URL
  val tunnelUrl = parameters.getOrElse("tunnelUrl", sys.error("Option 'tunnelUrl' not specified"))

  // the project name
  val project = parameters.getOrElse("project", sys.error("Option 'project' not specified"))

  // the table name
  val table = parameters.getOrElse("table", sys.error("Option 'table' not specified"))

  // describe the partition of the table, like pt=xxx,dt=xxx
  val partitionSpec = parameters.getOrElse("partitionSpec", null)

  // the number of partitions, default value is 1
  val numPartitions = parameters.getOrElse("numPartitions", "1").toInt

  // if allowed to create the specific partition which does not exist in table
  val allowCreateNewPartition = parameters.getOrElse("allowCreateNewPartition", "false").toBoolean

}
