package org.apache.spark.aliyun.maxcompute

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Created by songjun on 16/12/21.
 */
object Test {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        """Usage: TestOdps <accessKeyId> <accessKeySecret> <envType> <project> <table> <numPartitions>
          |
          |Arguments:
          |
          |    accessKeyId      Aliyun Access Key ID.
          |    accessKeySecret  Aliyun Key Secret.
          |    envType          0 or 1
          |                     0: Public environment.
          |                     1: Aliyun internal environment, i.e. Aliyun ECS etc.
          |    project          Aliyun ODPS project
          |    table            Aliyun ODPS table
          |    numPartitions    the number of RDD partitions
        """.stripMargin)
      System.exit(1)
    }

    val accessKeyId = args(0)
    val accessKeySecret = args(1)
    val envType = args(2).toInt
    val project = args(3)
    val table = args(4)
    val numPartitions = args(5).toInt

    val urls = Seq(
      Seq("http://service.odps.aliyun.com/api", "http://dt.odps.aliyun.com"), // public environment
      Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com") // Aliyun internal environment
    )

    val conf = new SparkConf().setAppName("Test Odps Read").setMaster("local")
    val ss = SparkSession.builder().appName("Test Odps Read").master("local").getOrCreate()




    // use jdbc to connect odps
    import ss.implicits._
    val odpsDF = ss.read
      .format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl","http://service.odps.aliyun.com/api")
      .option("tunnelUrl","http://dt.odps.aliyun.com")
      .option("table",table)
      .option("project",project)
      .option("accessKeySecret",accessKeySecret)
      .option("accessKeyId",accessKeyId).load()


    odpsDF.select("a").show


    val xxdf = ss.sparkContext.makeRDD((23 to 29).toSeq).toDF("a")

//    val properties = new Properties()
//    val url = "jdbc:odps:http://service.odps.aliyun.com/api"
//    properties.setProperty("driver", "com.aliyun.odps.jdbc.OdpsDriver")
//    properties.setProperty("project_name", project)
//    properties.setProperty("access_id", accessKeyId)
//    properties.setProperty("access_key", accessKeySecret)

    xxdf.write.format("org.apache.spark.aliyun.maxcompute.datasource")
      .option("odpsUrl","http://service.odps.aliyun.com/api")
      .option("tunnelUrl","http://dt.odps.aliyun.com")
      .option("table",table)
      .option("project",project)
      .option("accessKeySecret",accessKeySecret)
      .option("accessKeyId",accessKeyId).option("partitionSpec","b='haha'").mode(SaveMode.Append).save()

    println("abc")



    //    val odpsOps = envType match {
    //      case 0 =>
    //        OdpsOps(sc, accessKeyId, accessKeySecret, urls(0)(0), urls(0)(1))
    //      case 1 =>
    //        OdpsOps(sc, accessKeyId, accessKeySecret, urls(1)(0), urls(1)(1))
    //    }
    //
    //    writeTable(sc, odpsOps, project, table)
    //    val odpsData = odpsOps.readTable(project, table, read, numPartitions)
    //
    //    println("The top 10 elements are:")
    //    odpsData.top(10).foreach(println)
  }
}
