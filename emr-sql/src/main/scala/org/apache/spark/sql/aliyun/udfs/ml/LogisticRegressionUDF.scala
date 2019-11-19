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

package org.apache.spark.sql.aliyun.udfs.ml

import java.lang

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.json4s.DefaultFormats

import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.ParquetFormatModelMetadataLoader
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetFormatModelLoader
import org.apache.spark.sql.types._

class LogisticRegressionUDF extends GenericUDF with Logging {
  var _x1: StringObjectInspector = _
  var _x2: ObjectInspector = _
  var isVectorType = false

  override def getDisplayString(children: Array[String]): String = "Logistic_Regression"

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    if (arguments.length != 2) {
      throw new UDFArgumentException(
        s"""Logistic_Regression requires 2 arguments, got ${arguments.length}.
           |Arguments should be: (modelPath, features).
           |
           |  modelPath: LogisticRegression pre-trained model path in HDFS or OSS.
           |   features: data vector/string
         """.stripMargin)
    }

    val Array(x1, x2) = arguments

    if (!x1.isInstanceOf[StringObjectInspector]
        || (!x2.isInstanceOf[StructObjectInspector] && !x2.isInstanceOf[StringObjectInspector])) {
      val errorMsg =
        s"""Argument type error.
           |(modelPath: string, features: vector)
           |(${x1.isInstanceOf[StringObjectInspector]}, ${x2.isInstanceOf[StructObjectInspector]})
           |or
           |(modelPath: string, features: string)
           |(${x1.isInstanceOf[StringObjectInspector]}, ${x2.isInstanceOf[StringObjectInspector]})
        """.stripMargin
      logError(errorMsg)
      throw new UDFArgumentException(errorMsg)
    }

    _x1 = x1.asInstanceOf[StringObjectInspector]
    _x2 = x2 match {
      case _: StructObjectInspector =>
        isVectorType = true
        x2.asInstanceOf[StructObjectInspector]
      case _: StringObjectInspector =>
        x2.asInstanceOf[StringObjectInspector]
    }

    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
  }

  override def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val modelPath = _x1.getPrimitiveJavaObject(arguments(0).get())
    val model = LogisticRegressionUDF.loadModel(modelPath)
    val vector = if (isVectorType) {
      val features = _x2.asInstanceOf[StructObjectInspector]
        .getStructFieldsDataAsList(arguments(1).get())
      features.get(0).asInstanceOf[Byte] match {
        case 0 =>
          val size = features.get(1).asInstanceOf[Int]
          val indices = features.get(2).asInstanceOf[Array[Int]]
          val values = features.get(3).asInstanceOf[Array[Double]]
          new SparseVector(size, indices, values)
        case 1 =>
          val values = features.get(3).asInstanceOf[Array[Double]]
          new DenseVector(values)
      }
    } else {
      val line = _x2.asInstanceOf[StringObjectInspector].getPrimitiveJavaObject(arguments(1).get())
      val record = MLUtils.parseLibSVMRecord(line)
      new SparseVector(model.numFeatures, record._2, record._3)
    }

    new lang.Double(model.predict(vector))
  }
}

object LogisticRegressionUDF {
  var initialized: Boolean = false
  var model: LogisticRegressionModel = _
  val lock = new Object
  val className = "org.apache.spark.mllib.classification.LogisticRegressionModel"

  object VectorType extends VectorUDT

  val requiredSchema = StructType(Array(
    StructField("weights", VectorType),
    StructField("intercept", DoubleType),
    StructField("threshold", DoubleType)
  ))

  def loadModel(modelPath: String): LogisticRegressionModel = {
    lock.synchronized {
      if (!initialized) {
        val (loadedClassName, version, metadata) =
          ParquetFormatModelMetadataLoader.loadModelMetaData(modelPath)
        (loadedClassName, version) match {
          case (clazzName, "1.0") if clazzName == className =>
            implicit val formats = DefaultFormats
            val numFeatures = (metadata \ "numFeatures").extract[Int]
            val numClasses = (metadata \ "numClasses").extract[Int]
            val (weights, intercept, threshold) =
              ParquetFormatModelLoader.loadModelData(modelPath, className, requiredSchema)
            model = new LogisticRegressionModel(weights, intercept, numFeatures, numClasses)
            threshold match {
              case Some(t) => model.setThreshold(t)
              case None => model.clearThreshold()
            }
            initialized = true
          case _ => throw new Exception(
            s"ParquetFormatModelMetadataLoader.loadModel did not recognize model with " +
              s"(className, format version): ($loadedClassName, $version).  Supported:\n" +
              s"($className, 1.0)")
        }
      }
      model
    }
  }
}
