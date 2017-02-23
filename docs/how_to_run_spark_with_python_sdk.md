# Python Support

Bellow document will show you how to use Loghub (ODPS, ONS, MNS and so on) in pyspark. What calls for special attention is that E-MapReduce SDK and its dependencies should be included in class path. Try one of the following:

- Choice one

```
     Include the sdk library and its dependencies with in the
     spark-submit command as

     use ons as example:
     $ bin/spark-submit --packages com.aliyun.emr:emr-ons_2.10:${version} ...
```  

- Choice two

```
	 Download the JAR of the artifact from Maven Central http://search.maven.org/,
	 use ons as example:
     Group Id = com.aliyun.emr, Artifact Id = emr-ons_2.10, Version = ${version}.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <emr-ons_2.10-${version}.jar> ...
```

## LogService

1. download the [**loghub.py**](https://github.com/aliyun/aliyun-emapreduce-sdk/blob/master/sdk/src/main/python/pyspark/streaming/loghub.py);
2. download the pre-build [**emr-logservice_2.10 jar**](https://github.com/aliyun/aliyun-emapreduce-sdk/blob/master/prebuild);
3. download loghub's dependencies, i.e. [**fastjson-1.1.30.jar**](http://mvnrepository.com/artifact/com.alibaba/fastjson/1.1.30), [**commons-validator-1.4.0.jar**](http://mvnrepository.com/artifact/commons-validator/commons-validator/1.4.0), [**loghub-client-lib-0.6.4.jar**](http://mvnrepository.com/artifact/com.aliyun.openservices/loghub-client-lib/0.6.4), [**aliyun-log-0.6.2.jar**](http://mvnrepository.com/artifact/com.aliyun.openservices/aliyun-log/0.6.2). Above-mentioned library version pass test;
4. download [**loghub-wordcount.py**](https://github.com/aliyun/aliyun-emapreduce-sdk/blob/master/examples/src/main/python/streaming/loghub-wordcount.py);
5. run the example, use 
```
spark-submit --master local[4] --jars fastjson-1.1.30.jar,commons-validator-1.4.0.jar,
emr-logservice_2.10-1.4.0-SNAPSHOT.jar,loghub-client-lib-0.6.4.jar,aliyun-log-0.6.2.jar  
--py-files loghub.py loghub-wordcount.py <logServiceProject> <logsStoreName> <logHubConsumerGroupName> 
<loghubEndpoint> <numReceiver> <accessKeyId> <accessKeySecret>
```

## ODPS

1. download the [**odps.py**](https://github.com/aliyun/aliyun-emapreduce-sdk/blob/master/external/emr-maxcompute/src/main/python/pyspark/odps.py);
2. download the emr-sdk [**emr-sdk_2.10-1.3.2.jar**](http://central.maven.org/maven2/com/aliyun/emr/emr-sdk_2.10/1.3.2/emr-sdk_2.10-1.3.2.jar);
3. download odps's dependencies, i.e. [**aspectjrt-1.8.2.jar**](http://mvnrepository.com/artifact/org.aspectj/aspectjrt/1.8.2), [**odps-sdk-commons-0.20.7-public.jar**](http://mvnrepository.com/artifact/com.aliyun.odps/odps-sdk-commons/0.20.7-public), [**odps-sdk-core-0.20.7-public.jar**](http://mvnrepository.com/artifact/com.aliyun.odps/odps-sdk-core/0.20.7-public). Above-mentioned library version pass test;
4. download [**odps-sample.py**](https://github.com/aliyun/aliyun-emapreduce-sdk/blob/master/examples/src/main/python/odps-sample.py);
5. run the example, use
```
   spark-submit --master local[4] --jars aspectjrt-1.8.2.jar,emr-sdk_2.10-1.3.2.jar,
   odps-sdk-commons-0.20.7-public.jar,odps-sdk-core-0.20.7-public.jar --py-files odps.py  
   odps-sample.py <accessKeyId> <accessKeySecret> <project> <table> <partition> <numPartitions>
```
