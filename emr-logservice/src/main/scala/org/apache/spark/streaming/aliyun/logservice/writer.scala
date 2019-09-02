package org.apache.spark.streaming.aliyun.logservice

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

package object writer {
  /**
   * Convert a [[DStream]] to a [[LoghubWriter]] implicitly
   * @param dStream [[DStream]] to be converted
   * @return [[LoghubWriter]] ready to write messages to Loghub
   */
  implicit def dStreamToLoghubWriter[T: ClassTag, K, V](dStream: DStream[T]): LoghubWriter[T] =
    new DStreamLoghubWriter[T](dStream)

  /**
   * Convert a [[RDD]] to a [[LoghubWriter]] implicitly
   * @param rdd [[RDD]] to be converted
   * @return [[LoghubWriter]] ready to write messages to Loghub
   */
  implicit def rddToLoghubWriter[T: ClassTag, K, V](rdd: RDD[T]): LoghubWriter[T] =
    new RDDLoghubWriter[T](rdd)
}
