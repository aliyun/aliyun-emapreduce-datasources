package org.apache.spark.sql.execution.datasources.parquet

import java.util.TimeZone

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.RecordReaderIterator
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ParquetFormatModelLoader {
  def loadModelData(requiredSchema: StructType, modelPath: String): InternalRow = {
    val sqlConf = new SQLConf()
    val hadoopConf = new Configuration()

    val caseSensitiveAnalysis = sqlConf.caseSensitiveAnalysis
    val isParquetBinaryAsString = sqlConf.isParquetBinaryAsString
    val isParquetINT96AsTimestamp = sqlConf.isParquetINT96AsTimestamp

    val reader = new ParquetRecordReader[UnsafeRow](new ParquetReadSupport(None))
    val iter = new RecordReaderIterator(reader)

    val path = new Path(s"$modelPath/data")
    val fs = FileSystem.get(path.toUri, hadoopConf)
    val files = fs.listStatus(path).filter(_.isFile).filter(f => !f.getPath.getName.endsWith("_SUCCESS"))
    val fileStatus = files.head
    val split = new org.apache.parquet.hadoop.ParquetInputSplit(
      fileStatus.getPath, 0, fileStatus.getLen, fileStatus.getLen, Array.empty, null)

    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      TimeZone.getDefault.getID)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(requiredSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      isParquetINT96AsTimestamp)

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId)

    reader.initialize(split, hadoopAttemptContext)
    iter.asInstanceOf[Iterator[InternalRow]].next()
  }
}
