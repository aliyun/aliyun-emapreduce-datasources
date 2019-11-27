[![version](https://badge.fury.io/gh/aliyun%2Faliyun-emapreduce-sdk.png)](https://badge.fury.io/gh/aliyun%2Faliyun-emapreduce-sdk)
[![build](https://travis-ci.org/aliyun/aliyun-emapreduce-sdk.svg?branch=master-2.x)](https://travis-ci.org/aliyun/aliyun-emapreduce-sdk)

# E-MapReduce SDK

## Requirements

- Spark 1.3+

## Introduction

- This SDK supports interaction with Aliyun's base service, e.g. OSS, ODPS, LogService and ONS, in Spark runtime environment.

## Build and Install

```

	    git clone https://github.com/aliyun/aliyun-spark-sdk.git
	    cd  aliyun-spark-sdk
	    mvn clean package -DskipTests

```

#### Use SDK in Eclipse project directly

- copy sdk jar to your project
- right click Eclipse project -> Properties -> Java Build Path -> Add JARs
- choose and import the sdk
- you can use the sdk in your Eclipse project

#### Maven 

```
        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-maxcompute_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-logservice_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-tablestore</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-ons_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-datahub_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>
        
        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-jdbc_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>
        
        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-druid_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-hbase_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-kudu_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>
        
        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-redis_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-oss</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>
        
        <dependency>
            <groupId>com.aliyun.emr</groupId>
            <artifactId>emr-common_2.11</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>

```

## Run tests

* [How to run tests](docs/how_to_run_tests.md)

## OSS support

* [Hadoop/Spark on OSS](docs/aliyun_oss_support.md)

## MaxCompute support

* [Spark on MaxCompute](docs/aliyun_odps_support.md)

## ONS support

* [Spark on ONS](docs/aliyun_ons_support.md)

## LogService support

* [Spark on LogService](docs/aliyun_logservice_support.md)

## TableStore support

* [HadoopMR on TableStore](docs/HadoopMR-on-TableStore.md)
* [Spark on TableStore](docs/Spark-on-TableStore.md)
* [Hive/SparkSQL on TableStore](docs/Hive-SparkSQL-on-TableStore.md)

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
