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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.util.Utils

class DTSMicroBatchInputPartition(
    tp: TopicPartition,
    startOffset: Long,
    endOffset: Long,
    options: ju.Map[String, String]) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new DTSMicroBatchInputPartitionReader(tp, startOffset, endOffset, options)
}

private[dts] class DTSMicroBatchInputPartitionReader(
    tp: TopicPartition,
    startOffset: Long,
    endOffset: Long,
    options: ju.Map[String, String])
  extends InputPartitionReader[InternalRow] with Logging {
  private val pollTimeoutMs = options.getOrDefault("kafkaConsumer.pollTimeoutMs",
    (Utils.timeStringAsSeconds(
      SparkEnv.get.conf.get("spark.network.timeout", "120s")) * 1000L).toString).toLong

  private var nextRow: UnsafeRow = _
  private var nextOffset = startOffset
  private val fetchedData = FetchedData(
    ju.Collections.emptyListIterator[ConsumerRecord[Array[Byte], Array[Byte]]],
    -2L,
    startOffset)
  private val consumerConfig = DTSSourceProvider.sourceKafkaProperties(options)
  private val _consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfig)
  _consumer.assign(Seq(tp).asJava)

  private val converter = new DTSRecordToUnsafeRowConverter

  override def next(): Boolean = {
    if (!fetchedData.hasNext) {
      fetchData()
    }
    if (fetchedData.hasNext && nextOffset < endOffset) {
      val record = fetchedData.next()
      nextRow = converter.toUnsafeRow(record)
      nextOffset = record.offset + 1
      true
    } else {
      false
    }
  }

  private def fetchData(): Unit = {
    fetchedData.reset()
    _consumer.seek(tp, nextOffset)
    val r = _consumer.poll(pollTimeoutMs).records(tp)
    val offsetAfterPoll = r.get(r.size() - 1).offset()
    fetchedData.withNewPoll(r.listIterator, offsetAfterPoll)
  }

  override def get(): InternalRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    _consumer.close()
  }
}

private case class FetchedData(
    private var _records: ju.ListIterator[ConsumerRecord[Array[Byte], Array[Byte]]],
    private var _nextOffsetInFetchedData: Long,
    private var _offsetAfterPoll: Long) {

  def withNewPoll(
      records: ju.ListIterator[ConsumerRecord[Array[Byte], Array[Byte]]],
      offsetAfterPoll: Long): FetchedData = {
    this._records = records
    this._nextOffsetInFetchedData = -2L
    this._offsetAfterPoll = offsetAfterPoll
    this
  }

  def hasNext: Boolean = _records.hasNext

  def next(): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val record = _records.next()
    _nextOffsetInFetchedData = record.offset + 1
    record
  }

  def previous(): ConsumerRecord[Array[Byte], Array[Byte]] = {
    assert(_records.hasPrevious, "fetchedData cannot move back")
    val record = _records.previous()
    _nextOffsetInFetchedData = record.offset
    record
  }

  def reset(): Unit = {
    _records = ju.Collections.emptyListIterator()
    _nextOffsetInFetchedData = -2L
    _offsetAfterPoll = -2L
  }

  def nextOffsetInFetchedData: Long = _nextOffsetInFetchedData

  def offsetAfterPoll: Long = _offsetAfterPoll
}
