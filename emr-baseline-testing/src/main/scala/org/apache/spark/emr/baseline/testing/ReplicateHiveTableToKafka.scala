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

import scala.reflect.ClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ReplicateHiveTableToKafka extends Logging {

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)

  val SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url"
  val AUTO_REGISTER_SCHEMA = "auto.register.schemas"

  def main(args: Array[String]): Unit = {
    val Array(database, table, warehouseLocation, topic, schemaClass,
      bootstrapServers, schemaRegistryUrl, uuid, throughput, unbound) = args

    val spark = SparkSession.builder()
      .appName(s"Replicate table($table) to kafka topic($topic) - $uuid")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    sql(s"use $database")
    val df = sql(s"select * from $table").cache()
    while(true) {
      try {
        val rdd = schemaClass.toLowerCase match {
          case "catalogreturns" =>
            Utils.toRdd[CatalogReturns](df, classOf[CatalogReturns])
          case "catalogsales" =>
            Utils.toRdd[CatalogSales](df, classOf[CatalogSales])
          case "inventory" =>
            Utils.toRdd[Inventory](df, classOf[Inventory])
          case "storereturns" =>
            Utils.toRdd[StoreReturns](df, classOf[StoreReturns])
          case "storesales" =>
            Utils.toRdd[StoreSales](df, classOf[StoreSales])
          case "webreturns" =>
            Utils.toRdd[WebReturns](df, classOf[WebReturns])
          case "websales" =>
            Utils.toRdd[WebSales](df, classOf[WebSales])
        }

        Utils.send(rdd, topic, bootstrapServers, schemaRegistryUrl, throughput.toLong)
        if (!unbound.toBoolean) {
          return
        }

      } catch {
        case e: Throwable =>
          logWarning(s"Somethings wrong, but continue to replicate data.", e)
      } finally {
        Thread.sleep(100)
      }
    }
  }
}
