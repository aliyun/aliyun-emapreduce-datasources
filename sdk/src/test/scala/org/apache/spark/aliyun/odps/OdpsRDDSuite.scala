package org.apache.spark.aliyun.odps

import com.aliyun.odps.TableSchema
import com.aliyun.odps.data.Record
import org.apache.spark.{LocalSparkContext, SparkContext}
import org.apache.spark.aliyun.utils.OdpsUtils
import org.apache.spark.internal.Logging
import org.scalatest.FunSuite

class OdpsRDDSuite extends FunSuite with LocalSparkContext with Logging {

  private val accessKeyId: String = ""
  private val accessKeySecret: String = ""
  private val odpsUrl: String = ""
  private val tunnelUrl: String = ""
  private val project: String = ""
  private val table: String = ""
  private val pTable: String = ""
  private val partition: String = ""
  private val odpsUtils: OdpsUtils = OdpsUtils(accessKeyId, accessKeySecret, odpsUrl)

  test("odps table r/w") {
    odpsUtils.runSQL(project, s"drop table if exists $table;")
    odpsUtils.runSQL(project,
      s"create table if not exists $table(" +
        s"id string," +
        s"name string);")

    sc = new SparkContext("local[4]", "odps table r/w")
    val data = sc.parallelize(1 to 30, 1).map(l => (l.toString, (l * 30).toString))

    OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
      .saveToTable(project, table, data, OdpsSuite.write)

    val odpsRDD = OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
              .readTable(project, table, OdpsSuite.read, 2)

    assert(odpsRDD.count() == 30)
    val expectedRDD = data.join(odpsRDD).map(t => t._2._1.equals(t._2._2))
    assert(expectedRDD.count() == 30)
    assert(expectedRDD.filter(e => e).count() == 30)
  }

  test("odps partition table r/w") {
    odpsUtils.runSQL(project, s"drop table if exists $pTable;")
    odpsUtils.runSQL(project,
      s"create table if not exists $pTable(" +
        s"id string," +
        s"name string)" +
        s"partitioned by (month String);")

    sc = new SparkContext("local[4]", "odps partition table r/w")
    val data = sc.parallelize(1 to 30, 1).map(l => (l.toString, (l * 30).toString))

    OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
      .saveToTable(project, pTable, partition, data, OdpsSuite.write, true, true)

    val odpsRDD = OdpsOps(sc, accessKeyId, accessKeySecret, odpsUrl, tunnelUrl)
      .readTable(project, pTable, partition, OdpsSuite.read, 2)

    assert(odpsRDD.count() == 30)
    val expectedRDD = data.join(odpsRDD).map(t => t._2._1.equals(t._2._2))
    assert(expectedRDD.count() == 30)
    assert(expectedRDD.filter(e => e).count() == 30)
  }

}

object OdpsSuite {
  def read(record: Record, schema: TableSchema): (String, String) = {
    (record.getString(0), record.getString(1))
  }

  def write(kv: (String, String), emptyRecord: Record, schema: TableSchema) {
    emptyRecord.setString(0, kv._1)
    emptyRecord.setString(1, kv._2)
  }
}