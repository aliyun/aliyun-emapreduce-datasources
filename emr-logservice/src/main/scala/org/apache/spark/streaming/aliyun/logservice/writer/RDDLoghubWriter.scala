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

import scala.reflect.ClassTag

import com.aliyun.openservices.aliyun.log.producer.Callback
import com.aliyun.openservices.log.common.LogItem
import org.apache.commons.cli.MissingArgumentException

import org.apache.spark.rdd.RDD

class RDDLoghubWriter[T: ClassTag](@transient private val rdd: RDD[T])
  extends LoghubWriter[T] with Serializable {

  override def writeToLoghub(
      producerConfig: Map[String, String],
      topic: String,
      source: String,
      transformFunc: T => LogItem,
      callback: Option[Callback] = None): Unit =
    rdd.foreachPartition { partition =>
      val project = producerConfig.getOrElse("sls.project",
        throw new MissingArgumentException("Missing project (='sls.project')."))
      val logstore = producerConfig.getOrElse("sls.logstore",
        throw new MissingArgumentException("Missing logstore (='sls.logstore')."))
      val producer = CachedProducer.getOrCreate(project, producerConfig)
      partition
        .map(transformFunc)
        .foreach(record => {
          if (record != null) {
            producer.send(project, logstore, topic, source, record, callback.orNull)
          }
        })
    }
}
