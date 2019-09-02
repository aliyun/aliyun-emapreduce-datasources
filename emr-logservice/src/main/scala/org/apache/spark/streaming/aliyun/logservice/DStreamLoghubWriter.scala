package org.apache.spark.streaming.aliyun.logservice

import com.aliyun.openservices.aliyun.log.producer.Callback
import com.aliyun.openservices.log.common.LogItem
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag


class DStreamLoghubWriter[T: ClassTag](@transient private val dStream: DStream[T])
  extends LoghubWriter[T] with Serializable {

  override def writeToLoghub(producerConfig: Map[String, String],
                             topic: String,
                             source: String,
                             transformFunc: T => LogItem,
                             callback: Option[Callback] = None
                            ): Unit =
    dStream.foreachRDD { rdd =>
      val rddWriter = new RDDLoghubWriter[T](rdd)
      rddWriter.writeToLoghub(producerConfig, topic, source, transformFunc, callback)
    }
}