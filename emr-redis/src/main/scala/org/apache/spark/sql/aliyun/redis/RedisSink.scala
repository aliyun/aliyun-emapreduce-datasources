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
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.redis.DefaultSource

class RedisSink(sqlContext: SQLContext, sourceOptions: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val saveMode = sqlContext.sparkSession.conf
      .getOption("redis.save.mode")
      .getOrElse("append").toLowerCase match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case unknown: String => throw new Exception(s"Unknown redis save mode $unknown.")
    }
    new DefaultSource().createRelation(sqlContext, saveMode, sourceOptions, data)
  }
}
