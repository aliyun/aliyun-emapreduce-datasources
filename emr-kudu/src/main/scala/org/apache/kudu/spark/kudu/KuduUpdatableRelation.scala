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

package org.apache.kudu.spark.kudu

import org.apache.spark.annotation.{DeveloperApi, InterfaceStability}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class KuduUpdatableRelation(
    override val tableName: String,
    override val masterAddrs: String,
    override val operationType: OperationType,
    override val userSchema: Option[StructType],
    override val readOptions: KuduReadOptions = new KuduReadOptions,
    override val writeOptions: KuduWriteOptions = new KuduWriteOptions)(override val sqlContext: SQLContext)
  extends KuduRelation(tableName, masterAddrs, operationType, userSchema, readOptions, writeOptions)(sqlContext)
  with Serializable {

  @DeveloperApi
  @InterfaceStability.Evolving
  def merge(data: DataFrame, opTypeColumn: Column): Unit = {
    new KuduOperator(masterAddrs).writeRows(data, schema, opTypeColumn.toString(), tableName, writeOptions)
  }
}
