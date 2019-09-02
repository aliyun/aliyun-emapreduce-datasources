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
import com.aliyun.openservices.log.common.LogItem
import org.apache.commons.cli.MissingArgumentException


class ProducerAdapter(project: String,
                      logstore: String,
                      producer: Producer) {
  def send(topic: String,
           source: String,
           record: LogItem,
           callback: Callback): Unit = {
    producer.send(project, logstore, topic, source, record, callback)
  }

  def close(): Unit = {
    producer.close()
  }
}

object CachedProducer {
  @transient private var producer: ProducerAdapter = _

  def getOrCreate[K, V](producerConfig: Map[String, String]): ProducerAdapter = {
    if (producer == null) {
      val logProject = producerConfig.getOrElse("sls.project",
        throw new MissingArgumentException("Missing logService project (='sls.project')."))
      val logstore = producerConfig.getOrElse("sls.logstore",
        throw new MissingArgumentException("Missing logService project (='sls.logstore')."))
      val accessKeyId = producerConfig.getOrElse("access.key.id",
        throw new MissingArgumentException("Missing access key id (='access.key.id')."))
      val accessKeySecret = producerConfig.getOrElse("access.key.secret",
        throw new MissingArgumentException("Missing access key secret (='access.key.secret')."))
      val endpoint = producerConfig.getOrElse("sls.endpoint",
        throw new MissingArgumentException("Missing log store endpoint (='sls.endpoint')."))
      val config = new ProducerConfig()
      val maxIOThread = producerConfig.getOrElse("sls.ioThreadCount", "1").toInt
      config.setIoThreadCount(maxIOThread)
      val logProducer = new LogProducer(config)
      logProducer.putProjectConfig(new ProjectConfig(logProject, endpoint, accessKeyId, accessKeySecret))
      producer = new ProducerAdapter(logProject, logstore, logProducer)
      sys.addShutdownHook(producer.close())
    }
    producer
  }

  /**
   * Flush and close the cached [[Producer]]
   */
  def close(): Unit = {
    if (producer != null) {
      producer.close()
      producer = null
    }
  }
}
