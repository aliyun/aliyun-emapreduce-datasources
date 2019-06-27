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
package org.apache.spark.sql.aliyun.logservice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.util.{Utils => SUtils}

object LoghubWriter {
  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      sourceOptions: Map[String, String]): Unit = {
    val schema = queryExecution.analyzed.output
    // TODO: check the compatibility between output schema and logstore table.
    // Currently, we can output any data to logstore.
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask = new LoghubWriterTask(sourceOptions, schema)
      SUtils.tryWithSafeFinally(block = writeTask.execute(iter))(
        finallyBlock = writeTask.close())
    }
  }
}
