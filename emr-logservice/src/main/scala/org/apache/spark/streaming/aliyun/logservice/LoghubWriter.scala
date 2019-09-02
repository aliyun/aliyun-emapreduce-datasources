package org.apache.spark.streaming.aliyun.logservice

import com.aliyun.openservices.aliyun.log.producer.Callback
import com.aliyun.openservices.log.common.LogItem

import scala.reflect.ClassTag


abstract class LoghubWriter[T: ClassTag] extends Serializable {
  /**
   * Write a DStream to Loghub
   *
   * @param producerConfig producer configuration for creating KafkaProducer
   * @param transformFunc  a function used to transform values of T type into [[LogItem]]s
   * @param callback       an optional [[Callback]] to be called after each write, default value is None.
   */
  def writeToLoghub(producerConfig: Map[String, String],
                    topic: String,
                    source: String,
                    transformFunc: T => LogItem,
                    callback: Option[Callback] = None
                   ): Unit
}