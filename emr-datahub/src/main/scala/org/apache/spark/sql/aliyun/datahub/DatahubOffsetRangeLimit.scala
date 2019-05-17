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
package org.apache.spark.sql.aliyun.datahub

import java.util.Locale

sealed trait DatahubOffsetRangeLimit

case object OldestOffsetRangeLimit extends DatahubOffsetRangeLimit

case object LatestOffsetRangeLimit extends DatahubOffsetRangeLimit

case class SpecificOffsetRangeLimit(shardOffsets: Map[DatahubShard, Long]) extends DatahubOffsetRangeLimit

object DatahubOffsetRangeLimit {
  val LATEST = "-1" // indicates resolution to the latest offset
  val OLDEST = "-2" // indicates resolution to the oldest offset
  val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"

  def getOffsetRangeLimit(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: DatahubOffsetRangeLimit): DatahubOffsetRangeLimit = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffsetRangeLimit
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        OldestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(DatahubSourceOffset.partitionOffsets(json))
      case None => defaultOffsets
    }
  }
}