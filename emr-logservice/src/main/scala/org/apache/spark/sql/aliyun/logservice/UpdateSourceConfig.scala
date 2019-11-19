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

import joptsimple.OptionParser

object UpdateSourceConfig {
  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandOptions(args)
      if (args.length == 0) {
        // scalastyle:off
        println(opts.parser)
        // scalastyle:on
      }

      val zkConnectOpt = opts.options.valueOf(opts.zkConnectOpt)
      val checkpoint = opts.options.valueOf(opts.checkpoint)
      val logProject = opts.options.valueOf(opts.logProject)
      val logStore = opts.options.valueOf(opts.logStore)
      val config = opts.options.valueOf(opts.config).split(":")
      Utils.updateSourceConfig(zkConnectOpt, checkpoint, logProject, logStore, config(0), config(1))
    }
  }
}

class ConfigCommandOptions(args: Array[String]) {
  val parser = new OptionParser(false)
  val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the " +
    "zookeeper connection in the form host:port. Multiple URLS can be given to allow fail-over.")
    .withRequiredArg
    .describedAs("urls")
    .ofType(classOf[String])

  val checkpoint = parser.accepts("checkpoint-path", "The checkpoint path of spark streaming job.")
    .withRequiredArg
    .ofType(classOf[String])

  val logProject = parser.accepts("log-project", "Name of log project.")
    .withRequiredArg
    .ofType(classOf[String])

  val logStore = parser.accepts("log-store", "Name of log store.")
    .withRequiredArg
    .ofType(classOf[String])

  val config = parser.accepts("config", "configKey:configValue")
    .withRequiredArg
    .ofType(classOf[String])

  val helpOpt = parser.accepts("help", "Print usage information.")
  val options = parser.parse(args : _*)
}
