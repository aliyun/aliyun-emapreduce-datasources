In this doc, we will demonstrate how to consume ONS message in Spark.

```
    // cId: Aliyun ONS ConsumerID
    // topic: Message Topic
    // subExpression: Message Tag
    val Array(cId, topic, subExpression, parallelism, interval) = args

    val accessKeyId = "accessKeyId"
    val accessKeySecret = "accessKeySecret"

    val numStreams = parallelism.toInt
    val batchInterval = Milliseconds(interval.toInt)

    val conf = new SparkConf().setAppName("Spark ONS Sample")
    val ssc = new StreamingContext(conf, batchInterval)

    // define `func` to preprocess each message 
    def func: Message => Array[Byte] = msg => msg.getBody
    val onsStreams = (0 until numStreams).map { i =>
      println(s"starting stream $i")
      OnsUtils.createStream(
        ssc,
        cId,
        topic,
        subExpression,
        accessKeyId,
        accessKeySecret,
        StorageLevel.MEMORY_AND_DISK_2,
        func)
    }

    val unionStreams = ssc.union(onsStreams)
    unionStreams.foreachRDD(rdd => {
      rdd.map(bytes => new String(bytes)).flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _).collect().foreach(e => println(s"word: ${e._1}, cnt: ${e._2}"))
    })

    ssc.start()
    ssc.awaitTermination()
```