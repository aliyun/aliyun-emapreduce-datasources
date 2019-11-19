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
package org.apache.spark.streaming.aliyun.logservice.writer

import com.aliyun.openservices.aliyun.log.producer._
import org.apache.commons.cli.MissingArgumentException


object CachedProducer {
  @transient private var producer: Producer = _

  def getOrCreate[K, V](project: String,
                        producerConfig: Map[String, String]): Producer = {
    synchronized {
      if (producer == null) {
        val accessKeyId = producerConfig.getOrElse("access.key.id",
          throw new MissingArgumentException("Missing access key id (='access.key.id')."))
        val accessKeySecret = producerConfig.getOrElse("access.key.secret",
          throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
        val endpoint = producerConfig.getOrElse("sls.endpoint",
          throw new MissingArgumentException("Missing endpoint (='sls.endpoint')."))
        val config = new ProducerConfig()
        val maxIOThread = producerConfig.getOrElse("sls.ioThreadCount", "1").toInt
        config.setIoThreadCount(maxIOThread)
        producer = new LogProducer(config)
        producer.putProjectConfig(
          new ProjectConfig(project, endpoint, accessKeyId, accessKeySecret))
        sys.addShutdownHook(producer.close())
      }
      producer
    }
  }

  /**
   * Flush and close the cached [[Producer]]
   */
  def close(): Unit = {
    synchronized {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
  }
}
