package org.apache.spark.aliyun.maxcompute.datasource

/**
 * Created by songjun on 16/12/21.
 */
class ODPSOptions(
  @transient private val parameters: Map[String, String])
  extends Serializable {

  // Aliyun Account accessKeySecret
  val accessKeySecret = parameters.getOrElse("accessKeySecret", sys.error("Option 'accessKeySecret' not specified"))
  // Aliyun Account accessKeyId
  val accessKeyId = parameters.getOrElse("accessKeyId", sys.error("Option 'accessKeyId' not specified"))
  // the odps endpoint URL
  val odpsUrl = parameters.getOrElse("odpsUrl", sys.error("Option 'odpsUrl' not specified"))
  // the TableTunnel endpoint URL
  val tunnelUrl = parameters.getOrElse("tunnelUrl", sys.error("Option 'tunnelUrl' not specified"))
  // the project name
  val project = parameters.getOrElse("project", sys.error("Option 'project' not specified"))
  // the table name
  val table = parameters.getOrElse("table", sys.error("Option 'table' not specified"))
  // describe the partition of the table, like pt=xxx,dt=xxx
  val partitionSpec = parameters.getOrElse("partitionSpec", null)
  // the number of partitions, default value is 1
  val numPartitions = parameters.getOrElse("numPartitions", "1").toInt
  // if allowed to create the specific partition which does not exist in table
  val allowCreatNewPartition = parameters.getOrElse("allowCreatNewPartition", "false").toBoolean

}
