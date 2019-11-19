In this doc, we will demonstrate how to manipulate the Aliyun OSS data in Spark.

### OSS Extension - Native OSS FileSystem
A native way to read and write regular files on Aliyun OSS. The advantage of this way is you can access files on OSS that came from other Aliyun base service or other tools. But file in Aliyun OSS has 48.8TB limit.

### OSS URI

- **oss**://bucket/object/path

We can set OSS "AccessKeyId/AccessKeySecret/Endpoint" in Hadoop configuration:

```
    <property>
      <name>fs.oss.accessKeyId</name>
      <description>Aliyun access key ID</description>
    </property>
    
    <property>
      <name>fs.oss.accessKeySecret</name>
      <description>Aliyun access key secret</description>
    </property>
    
    <property>
      <name>fs.oss.endpoint</name>
      <description>Aliyun OSS endpoint to connect to. An up-to-date list is
        provided in the Aliyun OSS Documentation.
       </description>
    </property>
```
 
Specifically, we should add a prefix "spark.hadoop" in Spark, like "spark.hadoop.fs.oss.accessKeyId".

### OSS usage

Now, we provide a transparent way to support Aliyun OSS, with no code changes and just few configurations. All you need to do is just to provide two configuations in your project:

```

	conf.set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")


```

Then, you can load OSS data through `SparkContext.textFile(...)`, like:

```

	val conf = new SparkConf()
	conf.set("spark.hadoop.fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
	val sc = new SparkContext(conf)
	val path = "oss://bucket/input"
	val rdd = sc.textFile(path)

``` 

Similarly, you can upload data through `RDD.saveAsTextFile(...)`, like:

```

	val data = sc.parallelize(1 to 10)
	data.saveAsTextFile("oss://bucket/output")

```