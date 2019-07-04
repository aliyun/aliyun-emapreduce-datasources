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

package org.apache.spark.ml.util

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem}
import org.json4s.{DefaultFormats, JValue}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging

object ParquetFormatModelMetadataLoader extends Logging {
  def loadModelMetaData(modelPath: String): (String, String, JValue) = {
    var in: FSDataInputStream = null
    try {
      val metadataPath = new Path(modelPath, "metadata")
      val hadoopConf = new Configuration()
      val fs = FileSystem.get(metadataPath.toUri, hadoopConf)
      val files = fs.listStatus(metadataPath).filter(_.isFile).filter(f => !f.getPath.getName.endsWith("_SUCCESS"))
      val fileStatus = files.head
      in = fs.open(fileStatus.getPath)
      val bufferedReader = new BufferedReader(new InputStreamReader(in))

      implicit val formats = DefaultFormats
      val metadata = parse(bufferedReader.readLine())
      val clazz = (metadata \ "class").extract[String]
      val version = (metadata \ "version").extract[String]
      (clazz, version, metadata)
    } finally {
      if (in != null) {
        in.close()
      }
    }
  }
}
