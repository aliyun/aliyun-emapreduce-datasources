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

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{NextIterator, UninterruptibleThread}

class DTSSourceRDD(
    sc: SparkContext,
    sourceOptions: CaseInsensitiveMap[String],
    executorKafkaParams: Properties,
    startOffset: PartitionOffset,
    endOffset: PartitionOffset,
    pollTimeoutMs: Long) extends RDD[InternalRow](sc, Nil) {

  private val tp = {
    assert(startOffset._1 == endOffset._1)
    startOffset._1
  }

  @volatile protected var _consumer: Consumer[Array[Byte], Array[Byte]] = null

  protected def consumer: Consumer[Array[Byte], Array[Byte]] = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    if (_consumer == null) {
      _consumer = new KafkaConsumer[Array[Byte], Array[Byte]](executorKafkaParams)
      _consumer.assign(Seq(tp).asJava)
    }
    _consumer
  }

  override def persist(newLevel: StorageLevel): this.type = {
    logError("Kafka ConsumerRecord is not serializable. " +
      "Use .map to extract fields before calling .persist or .window")
    super.persist(newLevel)
  }

  override def compute(
      split: Partition,
      context: TaskContext): Iterator[InternalRow] = {
    val sourcePartition = split.asInstanceOf[DTSSourceRDDPartition]
    val range = resolveRange()
    val partitionReader = new DTSMicroBatchInputPartitionReader(
      sourcePartition.offsetRange._1, range._1, range._2, sourceOptions.asJava
    )

    if (range._1 == range._2) {
      logInfo(s"Beginning offset ${range._1} is the same as ending offset " +
        s"skipping $tp ${range._2}")
      closeKafkaConsumer()
      Iterator.empty
    } else {
      val underlying = new NextIterator[InternalRow]() {
        override def getNext(): InternalRow = {
          if (partitionReader.next()) {
            partitionReader.get()
          } else {
            finished = true
            null
          }
        }

        override protected def close(): Unit = {
          partitionReader.close()
          closeKafkaConsumer()
        }
      }
      // Release consumer, either by removing it or indicating we're no longer using it
      context.addTaskCompletionListener[Unit] { _ =>
        underlying.closeIfNeeded()
      }
      underlying
    }
  }

  override protected def getPartitions: Array[Partition] = {
    Array(DTSSourceRDDPartition(0, (tp, startOffset._2, endOffset._2)))
  }

  private def resolveRange() = {
    consumer.seekToBeginning(Set(tp).asJava)
    val earliestOffset = consumer.position(tp)
    consumer.seekToEnd(Set(tp).asJava)
    val latestOffset = consumer.position(tp)
    closeKafkaConsumer()
    if (startOffset._2 < 0 || endOffset._2 < 0) {
      val fromOffset = if (startOffset._2 < 0) {
        assert(startOffset._2 == DTSOffsetRangeLimit.EARLIEST,
          s"earliest offset ${startOffset._2} does not equal ${DTSOffsetRangeLimit.EARLIEST}")
        earliestOffset
      } else {
        startOffset._2
      }
      val untilOffset = if (endOffset._2 < 0) {
        assert(endOffset._2 == DTSOffsetRangeLimit.LATEST,
          s"latest offset ${endOffset._2} does not equal ${DTSOffsetRangeLimit.LATEST}")
        latestOffset
      } else {
        endOffset._2
      }
      (fromOffset, untilOffset)
    } else {
      (startOffset._2, endOffset._2)
    }
  }

  private def closeKafkaConsumer(): Unit = {
    if (_consumer != null) {
      _consumer.close()
      _consumer = null
    }
  }
}

private[dts] case class DTSSourceRDDPartition(
    index: Int,
    offsetRange: (TopicPartition, Long, Long)) extends Partition
