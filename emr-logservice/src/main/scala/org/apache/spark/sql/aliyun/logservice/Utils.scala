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

import com.aliyun.openservices.log.exception.LogException
import org.apache.commons.cli.MissingArgumentException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

object Utils extends Logging {
  def getSchema(schema: Option[StructType], sourceOptions: Map[String, String]): StructType = {
    validateOptions(sourceOptions)
    val logProject = sourceOptions("sls.project")
    val logStore = sourceOptions("sls.store")
    val endpoint = sourceOptions("endpoint")
    val accessKeyId = sourceOptions("access.key.id")
    val accessKeySecret = sourceOptions("access.key.secret")
    if (schema.isDefined && schema.get.nonEmpty) {
      schema.get
    } else {
      try {
        LoghubOffsetReader.loghubSchema(logProject, logStore,
          accessKeyId, accessKeySecret, endpoint)
      } catch {
        case e: LogException =>
          logWarning(s"Failed to analyse loghub schema, fall back to default " +
            s"schema ${LoghubOffsetReader.loghubSchema}", e)
          LoghubOffsetReader.loghubSchema
      }
    }
  }

  def validateOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    caseInsensitiveParams.getOrElse("sls.project",
      throw new MissingArgumentException("Missing logService project (='sls.project')."))
    caseInsensitiveParams.getOrElse("sls.store",
      throw new MissingArgumentException("Missing logService store (='sls.store')."))
    caseInsensitiveParams.getOrElse("access.key.id",
      throw new MissingArgumentException("Missing access key id (='access.key.id')."))
    caseInsensitiveParams.getOrElse("access.key.secret",
      throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
    caseInsensitiveParams.getOrElse("endpoint",
      throw new MissingArgumentException("Missing log store endpoint (='endpoint')."))
    caseInsensitiveParams.getOrElse("zookeeper.connect.address",
      throw new MissingArgumentException("Missing zookeeper connect address " +
        "(='zookeeper.connect.address')."))
  }
}
