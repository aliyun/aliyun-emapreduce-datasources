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

package org.apache.spark.sql.aliyun.dts

class DTSTestUtils {
  var sid: String = null
  var userName: String = null
  var password: String = null
  var bootstrapServer: String = null
  var topic: String = null

  var sourceProps: Map[String, String] = null

  def validateProps(): Unit = {
    sid = Option(System.getenv("SID_NAME")).getOrElse("")
    userName = Option(System.getenv("USER_NAME")).getOrElse("")
    password = Option(System.getenv("PASSWORD_NAME")).getOrElse("")
    bootstrapServer = Option(System.getenv("KAFKA_BROKER_URL_NAME")).getOrElse("")
    topic = Option(System.getenv("KAFKA_TOPIC")).getOrElse("")
  }

  validateProps()
}
