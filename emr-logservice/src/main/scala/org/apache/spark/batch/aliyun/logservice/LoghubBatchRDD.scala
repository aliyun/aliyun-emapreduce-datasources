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
package org.apache.spark.batch.aliyun.logservice

import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import com.aliyun.openservices.log.response.BatchGetLogResponse

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.aliyun.logservice.LoghubSourceProvider._
import org.apache.spark.streaming.aliyun.logservice.LoghubClientAgent
import org.apache.spark.util.NextIterator

class LoghubBatchRDD(
    @transient sc: SparkContext,
    project: String,
    logStore: String,
    accessId: String,
    accessKey: String,
    endpoint: String,
    startTime: Long,
    var endTime: Long = -1L,
    parallelismInShard: Int = 1) extends RDD[String](sc, Nil) {
  require(parallelismInShard >= 1 && parallelismInShard <= 5,
    "Parallelism in each shard should not be less than 1 or larger than 5.")

  def this(
      @transient sc: SparkContext,
      project: String,
      logStore: String,
      accessId: String,
      accessKey: String,
      endpoint: String, startTime: Long) = {
    this(sc, project, logStore, accessId, accessKey, endpoint, startTime, -1L)
  }

  @transient val client: LoghubClientAgent = LoghubBatchRDD.getClient(endpoint, accessId, accessKey)

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    try {
      val partition = split.asInstanceOf[ShardPartition]
      val client = LoghubBatchRDD.getClient(endpoint, accessId, accessKey)
      val it = new LoghubIterator(client, partition.shardId, project, logStore,
        partition.startCursor, partition.endCursor, context, partition.logGroupStep)
      new InterruptibleIterator[String](context, it)
    } catch {
      case _: Exception => Iterator.empty
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val shards = client.ListShard(project, logStore).GetShards()
    if (endTime == -1) {
      endTime = System.currentTimeMillis() / 1000
    }
    val rangeSize = endTime - startTime

    require(rangeSize >= parallelismInShard, s"The range between ($startTime, $endTime) " +
      s"is too small to split into $parallelismInShard slices.")
    val sliceSize = (rangeSize / parallelismInShard).toInt

    val ps = Array.tabulate(parallelismInShard - 1) { idx =>
      (idx, startTime + idx * sliceSize, startTime + (idx + 1) * sliceSize - 1)
    }

    val slices = if (ps.isEmpty) {
      Array((0, startTime, endTime))
    } else {
      val lastEndTime = ps(ps.length - 1)._2
      ps ++ Array((parallelismInShard - 1, lastEndTime + 1, endTime))
    }

    implicit val shardPartitionOrdering: Ordering[ShardPartition] =
      Ordering.by(t => (t.shardId, t.sliceId))
    // scalastyle:off
    import scala.collection.JavaConversions._
    // scalastyle:on
    shards.flatMap(shard => {
      val shardId = shard.GetShardId()
      slices.map { case ((idx, st, et)) =>
        val startCursor = client.GetCursor(project, logStore, shardId, st).GetCursor()
        val endCursor = client.GetCursor(project, logStore, shardId, et).GetCursor()
        val logGroupStep = sc.getConf.get("spark.loghub.batchGet.step", "100").toInt
        logInfo(s"Creating shard partition (shardId: $shardId, sliceId: $idx, startTime: $st, " +
          s"endTime: $et)")
        new ShardPartition(id, -1, shardId, idx, project, logStore, accessId, accessKey, endpoint,
          startCursor, endCursor, logGroupStep)
      }
    }).sorted.zipWithIndex.map { case (p, idx) =>
      p.updateIndex(idx)
    }.toArray
  }

  private class ShardPartition(
      rddId: Int,
      partitionId: Int,
      val shardId: Int,
      val sliceId: Int,
      project: String,
      logStore: String,
      accessKeyId: String,
      accessKey: String,
      endpoint: String,
      val startCursor: String,
      val endCursor: String,
      val logGroupStep: Int = 100) extends Partition {

    private var _index = partitionId

    override def hashCode(): Int = 41 * (41 + rddId) + shardId

    override def equals(other: Any): Boolean = super.equals(other)

    override def index: Int = _index

    def updateIndex(idx: Int): Partition = {
      _index = idx
      this
    }
  }

  private class LoghubIterator(
      client: LoghubClientAgent,
      shardId: Int,
      project: String,
      logStore: String,
      startCursor: String,
      endCursor: String,
      context: TaskContext,
      logGroupStep: Int) extends NextIterator[String] {
    var logCache = new LinkedBlockingQueue[String]()
    var curCursor: String = startCursor

    context.addTaskCompletionListener(context => closeIfNeeded())

    override protected def getNext(): String = {
      if (logCache.isEmpty) {
        if (curCursor.equals(endCursor)) {
          finished = true
          return ""
        }
        fetchData()
      }

      logCache.poll()
    }

    override protected def close(): Unit = {
      logCache.clear()
      logCache = null
    }

    def fetchData(): Unit = {
      val logData : BatchGetLogResponse = client.BatchGetLog(project, logStore, shardId,
        logGroupStep, curCursor, endCursor)

      // scalastyle:off
      import scala.collection.JavaConversions._
      // scalastyle:on
      logData.GetLogGroups().foreach(logGroup => {
        logGroup.GetLogGroup().getLogsList.foreach(log => {
          val data = new JSONObject()
          data.put(__TIME__, log.getTime)
          data.put(__TOPIC__, logGroup.GetTopic())
          data.put(__SOURCE__, logGroup.GetSource())
          log.getContentsList.foreach( content => {
            data.put(content.getKey, content.getValue)
          })
          val flg = logGroup.GetFastLogGroup()
          for (i <- 0 until flg.getLogTagsCount) {
            data.put("__tag__:".concat(flg.getLogTags(i).getKey), flg.getLogTags(i).getValue)
          }

          logCache.offer(data.toString)
        })
      })
      curCursor = logData.GetNextCursor()
    }
  }
}

object LoghubBatchRDD {
  private var client: LoghubClientAgent = null

  def getClient(endpoint: String, accessId: String, accessKey: String): LoghubClientAgent = {
    if (client == null) {
      client = new LoghubClientAgent(endpoint, accessId, accessKey)
    }

    client
  }
}
