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

package org.apache.spark.sql.druid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Partition, SparkContext, TaskContext}

class DruidSource(
    @transient sqlContext: SQLContext) extends Source{
  override def schema: StructType = new StructType()

  override def getOffset: Option[Offset] = Some(DruidOffset())

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    sqlContext.internalCreateDataFrame(new DruidRDD(sqlContext.sparkContext), schema, isStreaming = true)
  }

  override def stop(): Unit = {}
}

case class DruidOffset() extends Offset {
  override def json(): String = ""
}

class DruidRDD(@transient sc: SparkContext) extends RDD[InternalRow](sc, Nil) {

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
      Iterator.empty
    }
    override protected def getPartitions: Array[Partition] = Array(new ShardPartition())
}
class ShardPartition() extends Partition {
  override def hashCode(): Int = 0
  override def index: Int = 0
}
