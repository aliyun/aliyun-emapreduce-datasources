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
package org.apache.spark.sql.aliyun

/**
 * refer to [[org.apache.spark.sql.redis]] and used by
 * [[org.apache.spark.sql.aliyun.redis.RedisRelation]]
 */
package object redis {

  val RedisFormat = "org.apache.spark.sql.redis"

  val SqlOptionFilterKeysByType = "filter.keys.by.type"
  val SqlOptionNumPartitions = "partitions.number"
  /**
   * Default read operation number of partitions.
   */
  val SqlOptionNumPartitionsDefault = 3
  val SqlOptionTableName = "table"
  val SqlOptionKeysPattern = "keys.pattern"
  val SqlOptionKeysPatternForRewrite = "keys.pattern.for.rewrite"
  val SqlOptionModel = "model"
  val SqlOptionModelBinary = "binary"
  val SqlOptionModelHash = "hash"
  val SqlOptionInferSchema = "infer.schema"
  val SqlOptionKeyColumn = "key.column"
  val SqlOptionTTL = "ttl"

  val SqlOptionMaxPipelineSize = "max.pipeline.size"
  val SqlOptionScanCount = "scan.count"

  val SqlOptionIteratorGroupingSize = "iterator.grouping.size"
  val SqlOptionIteratorGroupingSizeDefault = 1000

  val StreamOptionStreamKeys = "stream.keys"
  val StreamOptionStreamOffsets = "stream.offsets"
  val StreamOptionReadBatchSize = "stream.read.batch.size"
  val StreamOptionReadBatchSizeDefault = 100
  val StreamOptionReadBlock = "stream.read.block"
  val StreamOptionReadBlockDefault = 500
  val StreamOptionParallelism = "stream.parallelism"
  val StreamOptionGroupName = "stream.group.name"
  val StreamOptionConsumerPrefix = "stream.consumer.prefix"
}
