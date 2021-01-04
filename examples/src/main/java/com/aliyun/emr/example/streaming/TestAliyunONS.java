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

package com.aliyun.emr.example.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.aliyun.ons.OnsUtils;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class TestAliyunONS {
  public static void main(String[] args) throws Exception{
    if (args.length < 6) {
      System.out.println("usage: TestAliyunONS <duration second> <consumer-id>"
          + " <topic> <tags> <access-key-id> <access-key>");
      System.exit(1);
    }

    long duration = Long.parseLong(args[0]) * 1000;
    String consumerId = args[1];
    String topic = args[2];
    String tags= args[3];
    String accessId = args[4];
    String accessKey = args[5];

    JavaStreamingContext jssc = new JavaStreamingContext(
        new SparkConf().setAppName("test-aliyun-ons"),
        new Duration(duration));
    OnsUtils.createDefaultStreams(
        jssc, consumerId, topic, tags, accessId, accessKey, StorageLevel.MEMORY_AND_DISK_2())
        .foreachRDD(new VoidFunction<JavaRDD<byte[]>>() {
          @Override
          public void call(JavaRDD<byte[]> javaRDD) throws Exception {
            javaRDD.foreach(new VoidFunction<byte[]>() {
              @Override
              public void call(byte[] bytes) throws Exception {
                System.out.println(new String(OnsUtils.toMessage(bytes).getBody()));
              }
            });
          }
        });
    jssc.start();
    jssc.awaitTermination();
  }
}
