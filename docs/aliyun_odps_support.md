In this doc, we will demonstrate how to manipulate the Aliyun ODPS data in Spark.

### Step-1. Initialize and OdpsOps
Before read/write ODPS data, we need to initialize an OdpsOps, like:


```

	import com.aliyun.odps.TableSchema
	import com.aliyun.odps.data.Record
	import org.apache.spark.aliyun.odps.OdpsOps
	import org.apache.spark.{SparkContext, SparkConf}
	
	object Sample {
	  def main(args: Array[String]): Unit = {	
	    // == Step-1 ==
	    val accessKeyId = "<accessKeyId>"
	    val accessKeySecret = "<accessKeySecret>"
		// intranet endpoints for example
	    val urls = Seq("http://odps-ext.aliyun-inc.com/api", "http://dt-ext.odps.aliyun-inc.com") 
	
	    val conf = new SparkConf().setAppName("Spark Odps Sample")
	    val sc = new SparkContext(conf)
	    val odpsOps = OdpsOps(sc, accessKeyId, accessKeySecret, urls(0), urls(1))

        // == Step-2 ==
		...
		// == Step-3 ==
		...
	  }

	  // == Step-2 ==
	  // function definition
	  // == Step-3 ==
	  // function definition
	｝

```

In above codes, the variables accessKeyId and accessKeySecret are assigned to users by system; they are named as ID pair, and used for user identification and signature authentication for OSS access. See [Aliyun AccessKeys](https://ak-console.aliyun.com/#/accesskey) for more information.

### Step-2. Load ODPS Data into Spark

```

		// == Step-2 ==
		val project = <odps-project>
		val table = <odps-table>
		val numPartitions = 2
		val inputData = odpsOps.readTable(project, table, read, numPartitions)
		inputData.top(10).foreach(println)

		// == Step-3 ==
		...

```

In above codes, we need to define a `read` function to pre-process ODPS data：

```

		def read(record: Record, schema: TableSchema): String = {
		  record.getString(0)
		}

```

It means to load ODPS table's first column into Spark.

### Step-3. Save results into Aliyun ODPS.

```

		val resultData = inputData.map(e => s"$e has been processed.")
		odpsOps.saveToTable(project, table, resultData, write)

```

In above codes, we need to define a `write` function to preprocess reslult data before write odps table：

```

		def write(s: String, emptyReord: Record, schema: TableSchema): Unit = {
		  val r = emptyReord
		  r.set(0, s)
		}

```

It means to write each line of result RDD into the first column of ODPS table.