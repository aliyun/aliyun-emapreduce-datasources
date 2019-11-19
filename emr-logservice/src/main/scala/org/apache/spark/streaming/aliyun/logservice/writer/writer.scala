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
package org.apache.spark.streaming.aliyun.logservice

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

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
