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
  def loadModelMetaData(modelPath: String, expectedClassName: String = ""): (String, String, JValue) = {
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
