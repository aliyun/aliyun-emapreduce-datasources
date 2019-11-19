## Running Tests

Tests are run by default via the [ScalaTest Maven plugin](http://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin).
The following is an example of a command to run the tests:

```
mvn clean test
``` 

## Running Individual Tests

```
mvn -Dtest=none -DwildcardSuites=x.x.x.class test
```

## Environment Variables For Tests

As there are some integration tests, we need to prepare some aliyun cloud resources before running tests:

| Environment variables | Desc| Required|
|---|---|---|
|ALIYUN_ACCESS_KEY_ID| The aliyun access key id | Yes|
|ALIYUN_ACCESS_KEY_SECRET|The aliyun access key secret | Yes |
|REGION_NAME| The region you want to run tests on. | Yes |
|TEST_ENV_TYPE| The testing network type, 'public' by default. 'public' means accessing aliyun cloud through public network environment. 'private' means accessing aliyun cloud through aliyun private network environment.| Yes|
|LOGSTORE_PROJECT_NAME|The name of logstore project| Required for logservice datasource tests|
|OTS_INSTANCE_NAME| The name of tablestore intance | Required for tablestore datasource tests|
|DATAHUB_PROJECT_NAME| The name of datahub project | Required for datahub datasource tests|
|ODPS_PROJECT_NAME| The name of maxcompute project | Required for maxcompute datasource tests |

#### Set These Environment Variables

There are three ways to set these environment variables:

- Set them in `bash_profile`
```
vi ~/.bash_profile

export ALIYUN_ACCESS_KEY_ID=xxx
export ALIYUN_ACCESS_KEY_SECRET=xxx
export REGION_NAME=cn-hangzhou
export TEST_ENV_TYPE=public
export LOGSTORE_PROJECT_NAME=xxx
export OTS_INSTANCE_NAME=xxx
export DATAHUB_PROJECT_NAME=xxx
export ODPS_PROJECT_NAME=xxx
```
- Set them in command line
```
ALIYUN_ACCESS_KEY_ID=xxx ALIYUN_ACCESS_KEY_SECRET=xxx REGION_NAME=cn-hangzhou TEST_ENV_TYPE=public
LOGSTORE_PROJECT_NAME=xxx OTS_INSTANCE_NAME=xxx DATAHUB_PROJECT_NAME=xxx ODPS_PROJECT_NAME=xxx mvn clean test
```
- Set them in `Run/Debug Configuration` in IntelliJ IDEA/Eclipse: Then you can run or debug one unit test in IntelliJ IDEA/Eclipse

## Run All Tests

The following is an example of a command to run all tests:

```
ALIYUN_ACCESS_KEY_ID=xxx ALIYUN_ACCESS_KEY_SECRET=xxx REGION_NAME=cn-hangzhou TEST_ENV_TYPE=public
LOGSTORE_PROJECT_NAME=xxx OTS_INSTANCE_NAME=xxx DATAHUB_PROJECT_NAME=xxx ODPS_PROJECT_NAME=xxx mvn clean test
```

## Run All Tests In One Module

The following is an example of a command to run all tests in `emr-logservice`:

```
ALIYUN_ACCESS_KEY_ID=xxx ALIYUN_ACCESS_KEY_SECRET=xxx REGION_NAME=cn-hangzhou TEST_ENV_TYPE=public
LOGSTORE_PROJECT_NAME=xxx mvn -pl emr-logservice clean test
```

## Run Individual Tests

```
ALIYUN_ACCESS_KEY_ID=xxx ALIYUN_ACCESS_KEY_SECRET=xxx REGION_NAME=cn-hangzhou TEST_ENV_TYPE=public
LOGSTORE_PROJECT_NAME=xxx mvn -Dtest=none -DwildcardSuites=org.apache.spark.sql.aliyun.logservice.LoghubSourceOffsetSuite test
```