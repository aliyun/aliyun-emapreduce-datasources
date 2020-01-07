In this doc, we will demonstrate how to consume Loghub data in Spark Streaming.

```
    if (args.length < 8) {
      System.err.println(
        """Usage: TestLoghub <sls project> <sls logstore> <loghub group name> <sls endpoint> <access key id>
          |         <access key secret> <receiver number> <batch interval seconds>
        """.stripMargin)
      System.exit(1)
    }

    val logserviceProject = args(0)    // The project name in your LogService.
    val logStoreName = args(1)         // The name of of logstream.
    val loghubGroupName = args(2)      // Processes with the same loghubGroupName will consume data of logstream together.
    val loghubEndpoint = args(3)       // API endpoint of LogService 
    val accessKeyId = args(4)          // AccessKeyId
    val accessKeySecret = args(5)      // AccessKeySecret
    val numReceivers = args(6).toInt   
    val batchInterval = Milliseconds(args(7).toInt * 1000) 

    val conf = new SparkConf().setAppName("Test Loghub")
    val ssc = new StreamingContext(conf, batchInterval)
    val loghubStream = LoghubUtils.createStream(
      ssc,
      loghubProject,
      logStream,
      loghubGroupName,
      endpoint,
      numReceivers,
      accessKeyId,
      accessKeySecret,
      StorageLevel.MEMORY_AND_DISK)

    loghubStream.foreachRDD(rdd => println(rdd.count()))

    ssc.start()
    ssc.awaitTermination()
```