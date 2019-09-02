package org.apache.spark.streaming.aliyun.logservice

import com.aliyun.openservices.aliyun.log.producer.Callback
import com.aliyun.openservices.log.common.LogItem
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class RDDLoghubWriter[T: ClassTag](@transient private val rdd: RDD[T])
  extends LoghubWriter[T] with Serializable {

  override def writeToLoghub(producerConfig: Map[String, String],
                             topic: String,
                             source: String,
                             transformFunc: T => LogItem,
                             callback: Option[Callback] = None
                            ): Unit =
    rdd.foreachPartition { partition =>
      val producer = CachedProducer.getOrCreate(producerConfig)
      partition
        .map(transformFunc)
        .foreach(record => producer.send(topic, source, record, callback.orNull))
    }
}