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
package org.apache.spark.sql.aliyun.redis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.StructType

class RedisSink(sqlContext: SQLContext, sourceOptions: Map[String, String]) extends Sink with Logging {
  private var initialed = false

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val schema = data.schema
    val encoder = RowEncoder(schema).resolveAndBind()
    val rdd = data.queryExecution.toRdd.map(r => encoder.fromRow(r))
    val df = sqlContext.sparkSession.createDataFrame(rdd, schema)

    val relation = new RedisRelation(sqlContext, sourceOptions, None, batchId)
    if (!initialed && !existKeyColumn(schema, sourceOptions)) {
      relation.insert(df, overwrite = true)
    } else {
      val saveMode = sqlContext.sparkSession.conf
        .getOption("redis.save.mode")
        .getOrElse("append").toLowerCase
      saveMode match {
        case "append" => relation.insert(df, overwrite = false)
        case "overwrite" => relation.insert(df, overwrite = true)
        case "errorifexists" =>
          if (relation.nonEmpty) {
            throw new IllegalStateException("SaveMode is set to ErrorIfExists and dataframe " +
              "already exists in Redis and contains data.")
          }
          relation.insert(df, overwrite = false)
        case "ignore" =>
          if (relation.isEmpty) {
            relation.insert(df, overwrite = false)
          }
        case unknown: String => throw new Exception(s"Unknown redis save mode $unknown.")
      }
    }

    initialed = true
  }

  private def existKeyColumn(schema: StructType, sourceOptions: Map[String, String]): Boolean = {
    sourceOptions.contains(SqlOptionKeyColumn) &&
      schema.fieldNames.exists(_.equals(sourceOptions(SqlOptionKeyColumn)))
  }
}