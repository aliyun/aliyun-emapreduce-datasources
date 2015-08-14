# Spark on Aliyun

## Requirements

- Spark1.3+

## Introduction

- This SDK supports interaction with Aliyun's base service, e.g. OSS and ODPS, in Spark runtime environment.

## Build and Install

```

		git clone https://github.com/aliyun/aliyun-spark-sdk.git
	    cd  aliyun-spark-sdk
	    mvn clean package -Dmaven.test.skip=true

```

#### Use SDK in Eclipse project directly

- copy `amr-sdk-<version>.jar` to your project
- right click Eclipse project -> Properties -> Java Build Path -> Add JARs
- choose and import the sdk
- you can use the sdk in your Eclipse project

#### Maven 

You need to install the SDK into local maven repository and add following dependency.

```

		mvn install:install-file -Dfile=amr-sdk-<version>.jar -DgroupId=com.aliyun -DartifactId=amr-sdk_2.10 -Dversion=0.1.0 -Dpackaging=jar

        <dependency>
            <groupId>com.aliyun</groupId>
            <artifactId>amr-sdk_2.10</artifactId>
            <version>0.1.0</version>
        </dependency>

```

## OSS Support

In this section, we will demonstrate how to manipulate the Aliyun OSS data in Spark.

### Step-1. Initialize an OssOps
Before read/write OSS data, we need to initialize an OssOps, like:


```

	import org.apache.spark.{SparkConf, SparkContext}
	import org.apache.spark.aliyun.oss.OssOps
	
	object Sample {
	  def main(args: Array[String]): Unit = {
		// == Step-1 ==
	    val accessKeyId = "<accessKeyId>"
	    val accessKeySecret = "<accessKeySecret>"
		// hangzhou for example
	    val endpoint = "http://oss-cn-hangzhou.aliyuncs.com"
	
	    val conf = new SparkConf().setAppName("Spark OSS Sample")
		// three important confirguation
	    conf.set("spark.hadoop.fs.oss.accessKeyId", accessKeyId)
	    conf.set("spark.hadoop.fs.oss.accessKeySecret", accessKeySecret)
	    conf.set("spark.hadoop.fs.oss.endpoint", endpoint)

		val sc = new SparkContext(conf)
		val ossOps = OssOps(sc, endpoint, accessKeyId, accessKeySecret)

        // == Step-2 ==
		...
		// == Step-3 ==
		...
	  }
	}

```

In above codes, the variables accessKeyId and accessKeySecret are assigned to users by system; they are named as ID pair, and used for user identification and signature authentication for OSS access. See [Aliyun AccessKeys](https://ak-console.aliyun.com/#/accesskey) for more information.

### Step-2. Load OSS Data into Spark.

```

		// == Step-2 ==
        val inputPath = "ossn://bucket-name/input/path"
		val numPartitions = 2
		val inputData = ossOps.readOssFile(inputPath, numPartitions)
		inputData.top(10).foreach(println)

		// == Step-3 ==
        ...

```

### Step-3. Save results into Aliyun OSS.

```

		// Step-3
		val outputPath = "ossn://bucket-name/output/path"
		val resultData = inputData.map(e => s"$e has been processed.")
		ossOps.saveToOssFile(outputPath, resultData)

```

### OSS Extension
This SDK support two kinds of filesystem clients for reading and writing from and to Aliyun OSS, i.e.

- Native OSS FileSystem： A native way to read and write regular files on Aliyun OSS. The advantage of this way is you can access files on OSS that came from other Aliyun base service or other tools. But file in Aliyun OSS has 48.8TB limit.
- Block-based OSS FileSystem：This allows Aliyun OSS to supports larger files (no limit theoretically). File in Aliyun OSS is organized with many blocks. Each block is an Aliyun OSS object, and the block size is configurable, i.e. reuse the Hadoop's `fs.local.block.size` property. The disadvantage is that it can not interoperable with other Aliyun OSS tools.

Now, we only support two ways to read and write Aliyun OSS data:

- read by using `Native OSS FileSystem`, and write by using `Block-based OSS FileSystem`
- read by using `Block-based OSS FileSystem`, and write by usring `Block-based OSS FileSystem`

### OSS URI

We support different types of URI for each filesystem client:

- Native URI： **ossn**://bucket-name/object/path
- Block-based URI: **oss**://bucket-name/object/path


## ODPS Support

In this section, we will demonstrate how to manipulate the Aliyun ODPS data in Spark.

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

In above codes, we need to define a `read` function to preprocess ODPS data：

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

## Advanced  Usage

Now, we provide a transparent way to support Aliyun OSS, with no code changes and just few configurations. All you need to do is just to provide two configuations in your project:

```

	conf.set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.blk.OssFileSystem")
    conf.set("spark.hadoop.fs.ossn.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")


```

If only use `Native OSS` or `Block-Based OSS`, you just need to add the corresponding configuration. Then, you can load OSS data through `SparkContext.textFile(...)`, like:

```

	val conf = new SparkConf()
    conf.set("spark.hadoop.fs.oss.accessKeyId", "accessKeyId")
    conf.set("spark.hadoop.fs.oss.accessKeySecret", "accessKeySecret")
    conf.set("spark.hadoop.fs.oss.endpoint", "endpoint")
    conf.set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.blk.OssFileSystem")
    conf.set("spark.hadoop.fs.ossn.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
    val sc = new SparkContext(conf)
	
	val path1 = "oss://bucket/path1"
	val rdd1 = sc.textFile(path1)

	val path2 = "ossn://bucket/path2"
	val rdd2 = sc.textFile(path2)

``` 

Similarly, you can upload data through `RDD.saveAsTextFile(...)`, like:

```

	val data = sc.parallelize(1 to 10)
	data.saveAsTextFile("ossn://bucket/path3")

```

**Attention**: now we only support saving to **native** URI through `RDD.saveAsTextFile(...)`.

## Future Work

- Support more Aliyun base service, like OTS, ONS and so on.
- Support more friendly code migration.

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)