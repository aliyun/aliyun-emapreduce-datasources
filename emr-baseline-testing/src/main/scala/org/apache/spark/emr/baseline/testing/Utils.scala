/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  * <p>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.spark.emr.baseline.testing

import java.nio.ByteBuffer
import java.util.{Calendar, Properties, TimeZone}

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.tools.ThroughputThrottler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}

import scala.reflect.ClassTag
import scala.util.Random

object Utils {
  def send[T](
      rdd: RDD[T],
      topic: String,
      bootstrapServers: String,
      schemaRegistryUrl: String,
      throughput: Long): Unit = {

    rdd.mapPartitions(it => {
      println(s"Throughput in each partition is $throughput")

      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      props.put(ProducerConfig.ACKS_CONFIG, "all")
      props.put(ProducerConfig.RETRIES_CONFIG, "0")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
      props.put(ReplicateHiveTableToKafka.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)
      props.put(ReplicateHiveTableToKafka.AUTO_REGISTER_SCHEMA, "true")

      val startMs = System.currentTimeMillis

      val throttler = new ThroughputThrottler(throughput, startMs)
      try {
        val producer = new KafkaProducer[String, T](props)
        var num: Long = 0
        it.foreach(e =>
          try {
            val sendStartMs = System.currentTimeMillis
            val record = new ProducerRecord[String, T](topic, null, e)
            producer.send(record)
            num += 1
            if (throttler.shouldThrottle(num, sendStartMs)) {
              throttler.throttle()
            }
          } catch {
            case e: Exception =>
              e.printStackTrace()
          }
        )
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }

      Iterator.empty.asInstanceOf[Iterator[String]]
    }).count()
  }

  def toBytes(value: java.math.BigDecimal): ByteBuffer =
    ByteBuffer.wrap(value.unscaledValue.toByteArray)

  def toRdd[T <: SpecificRecordBase : ClassTag ](df: DataFrame, clazz: Class[_]): RDD[T] = {
    df.rdd.map(row => {
      val cons = clazz.getConstructor()
      val instance = cons.newInstance().asInstanceOf[T]
      var nextIdx: Int = 0
      row.schema.fields.zipWithIndex.foreach { case (field, idx) =>
        val value = field.dataType match {
          case LongType =>
            if (row.isNullAt(idx)) {
              null
            } else {
              row.getLong(idx)
            }
          case _: DecimalType =>
            if (row.isNullAt(idx)) {
              null
            } else {
              Utils.toBytes(row.getDecimal(idx))
            }
          case IntegerType =>
            if (row.isNullAt(idx)) {
              null
            } else {
              row.getInt(idx)
            }
        }

        instance.put(idx, value)
        nextIdx = idx + 1
      }
      val rand1 = Random.nextInt(100)
      val rand2 = Random.nextInt(300)
      // 5% data is delayed
      val delay = if (rand1 < 5) {
        // max delay time is 5 min
        rand2
      } else {
        0
      }
      val dataTime = Calendar.getInstance(TimeZone.getTimeZone("GMT+8")).getTime.getTime - delay * 1000L
      instance.put(nextIdx, dataTime)
      instance
    })
  }
}
