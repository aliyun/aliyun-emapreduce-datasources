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
package com.aliyun.emr.examples.datagen

import java.util

import scala.collection.JavaConverters._
import scala.util.Random

import com.aliyun.datahub.{DatahubClient, DatahubConfiguration}
import com.aliyun.datahub.auth.AliyunAccount
import com.aliyun.datahub.common.data.FieldType
import com.aliyun.datahub.model.RecordEntry

import org.apache.spark.sql.types.{DataTypes, StructField}

object DatahubDataGen {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      // scalastyle:off
      System.err.println(
        "Usage: DatahubDataGen <project> <topic> <access-key-id> <access-key-secret>")
      // scalastyle:on
      System.exit(1)
    }

    val Array(project, topic, endpoint, accessKeyId, accessKeySecret) = args

    val dclient = new DatahubClient(new DatahubConfiguration(
      new AliyunAccount(accessKeyId, accessKeySecret), endpoint))
    val schema = dclient.getTopic(project, topic).getRecordSchema

    while(true) {
      val recordEntries = new util.ArrayList[RecordEntry]()
      for(i <- 0 to 100) {
        val data = new RecordEntry(schema)
        schema.getFields.asScala.foreach(f => {
          f.getType match {
            case FieldType.STRING => data.setString(f.getName, "i am a string")
            case FieldType.BIGINT => data.setBigint(f.getName, Random.nextLong())
            case FieldType.BOOLEAN => data.setBoolean(f.getName, Random.nextBoolean())
            case FieldType.DECIMAL => data.setDecimal(f.getName, java.math.BigDecimal.valueOf(0.1d))
            case FieldType.DOUBLE => data.setDouble(f.getName, Random.nextDouble())
            case FieldType.TIMESTAMP => data.setTimeStamp(f.getName, System.currentTimeMillis())
            case _ => StructField(f.getName, DataTypes.StringType)
          }
        })
        recordEntries.add(data)
      }

      dclient.putRecords(project, topic, recordEntries)
    }
  }
}
