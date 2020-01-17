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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.kafka.common.TopicPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization


object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  def partitionOffsets(str: String): PartitionOffset = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
        partOffsets.map { case (part, offset) =>
          (new TopicPartition(topic, part), offset)
        }
      }.head
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23}}, got $str""")
    }
  }

  def partitionOffsets(partitionOffset: PartitionOffset): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    result.put(partitionOffset._1.topic(),
      HashMap(partitionOffset._1.partition() -> partitionOffset._2))
    Serialization.write(result)
  }
}
