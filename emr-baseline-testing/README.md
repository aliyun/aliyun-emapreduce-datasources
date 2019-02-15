## Spark Streaming SQL Baseline Testing

This is a baseline testing framework for Spark Streaming SQL (based on Spark Structured Streaming) upon Spark 2.3+.

**Note: This README is still under development. Please also check our source code for more information.**

## Quick Start

### Precondition

- Before running any queries, we should setup some data streams. To simplify data generation, we choose to replicate TPC-DS dataset
to Kafka. 
- How to generate TPC-DS dataset? Refer to ["TPC-DS: Setup a benchmark"](https://github.com/databricks/spark-sql-perf#setup-a-benchmark).
Generally, there is no need to generate a large dataset. We suggest 1G~10G is enough.
- Where to run queries? We will provide a preview "Spark Streaming SQL" implementation in E-MapReduce in the coming version. For 
more information please pay close attention to official documentation: [E-MapReduce release note](https://help.aliyun.com/document_detail/31814.html).    

### Running from command line.

```
1. Submit jobs to replicate TPC-DS dataset to Kafka:
$ ./bin/start-data-simulator.sh -database tpcds_hdfs_text_10 -tables store_sales,store_returns -warehouse hdfs:///user/hive/warehouse -bootstrapServers a.b.c.d:9092 -schemaRegistryUrl http://a.b.c.d:8081 -throughput 10000 -unbound

Usage: start-data-simulator.sh -database <database> -tables <tables> -warehouse <warehouseLocation> -bootstrapServers <bootstrapServers> -schemaRegistryUrl <schemaRegistryUrl> -throughput throughput [-unbound]

           <database>: the name of TPC-DS dataset database
             <tables>: the name of tables to be replicated to Kafka, separated by commas.
  <warehouseLocation>: the location path of hive warehouse
   <bootstrapServers>: the bootstrap servers of Kafka
  <schemaRegistryUrl>: the url of Kafka schema registry
         <throughput>: the max data throughput in each spark task.
            <unbound>: indicate if continue after replicate whole table data. If true, replicate table data circularly, otherwise replicate table data just once. 

You can also use `start-all-data-simulator.sh` script to replicate all needed data to Kafka circularly。

2. Stop all submited data replicating jobs: 
$ ./bin/stop-data-simulator.sh

3. Create Spark stream tables whose source is Kafka and initialize topic (of result table) schema:
$ ./bin/load.sh

There are some env configurations we need to set in ./bin/config.sh
                 <SF>: the scale factor ogf TPC-DS dataset, "10" default.
      <WAREHOUSE_DIR>: the location path of hive warehouse, "hdfs:///user/hive/warehouse" default.
              <STORE>: the storage system, "hdfs" default.
               <PORT>: the port of Spark Thrift Server, "10001" default.
                 <DB>: the name of TPC-DS dataset database.
             <TABLES>: the list of tables, "store_returns,store_sales,web_returns,web_sales,inventory,catalog_returns,catalog_sales" default.
  <BOOTSTRAP_SERVERS>: the bootstrap servers of Kafka.
<SCHEMA_REGISTRY_URL>: the url of Kafka schema registry.
    <CHECKPOINT_ROOT>: the spark streaming checkpoint root directory, "hdfs:///user/spark/sql/streaming/checkpoint" default.
         <THROUGHPUT>: the throughput of data replicating in each spark task, "10000" default.
 <TESTING_TIMEOUT_MS>: the spark streaming query timeout in ms if `TESTING_ENABLE` is true.
     <TESTING_ENABLE>: indicate if enable spark test mode. If true, the spark streaming query will be submited in single spark-sql client with "TESTING_TIMEOUT_MS" timeout.
                       If false, the spark streaming query will be submited to spark thrift server in a long-running way. So if "TESTING_ENABLE" is false, we should start
                       a spark thrift server first.
      <NUM_EXECUTORS>: the number of executors in spark-sql job, if "TESTING_ENABLE" is true.
     <EXECUTOR_CORES>: the core number of executor in spark-sql job, if "TESTING_ENABLE" is true.
    <EXECUTOR_MEMORY>: the memory size of executor in spark-sql job, if "TESTING_ENABLE" is true.

4. Run a query:
$ ./bin/run-query.sh 3

Usage: run-query.sh <queryId>

            <queryId>: the id of qurey, '3' means the 'q3.sql' in ./queries directory.
``` 

5. Run all queries.
$ ./bin/run-all.sh

### Build

```
$ ./dev/build.sh
```

This command will create a emr-baseline-testing-dist-`<version>`.tgz in module root. This distribution package contains:
- bin: command tools
- lib: tool dependency libraries
- queries: baseline testing queries
- tables: baseline testing table definitions
- schemas: result topic schema definition.

### Data Stream

Just like said above, the testing data stream comes from replicating TPC-DS dataset. These data keeps the original 
schema (name and type). In consideration of particularity for stream query, we introduced:
- Add a Timestamp type column.

| Table | Added Timestamp Column|
|---|---|
|catalog_returns| cr_data_time|
|catalog_sales| cs_data_time|
|inventory| inv_data_time|
|store_returns| sr_data_time|
|store_sales| ss_data_time|
|web_returns| wr_data_time|
|web_sales| ws_data_time|

- Simulate data delay
  - delayed data percentage: <= 5%
  - max data delay: 5 minutes

### DDL

- without schema definition

Spark will retrieve the table schema from kafka schema registry.

```
CREATE TABLE driverbehavior 
USING kafka 
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = "${TOPIC_NAME}",
output.mode = "${OUTPUT_MODE}"",
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = "${SCHEMA_RECORD_NAME}",
kafka.schema.record.namespace = "${SCHEMA_RECORD_NAMESPACE}",
kafka.auto.register.schemas = "${AUTO_REGISTER_SCHEMA_ENABLE}");
```

- with user-defined schema

The user-defined schema must be the subset of schema which is registered in kafka schema registry.

```
CREATE TABLE driverbehavior(deviceId string, velocity double)
USING kafka 
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = "${TOPIC_NAME}",
output.mode = "${OUTPUT_MODE}"",
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = "${SCHEMA_RECORD_NAME}",
kafka.schema.record.namespace = "${SCHEMA_RECORD_NAMESPACE}",
kafka.auto.register.schemas = "${AUTO_REGISTER_SCHEMA_ENABLE}");
```

| Config|definition|
|---|---|
|kafka.bootstrap.servers|the bootstrap servers of kafka.|
|subscribe| the topic name to subscribe.|
|output.mode| specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.|
|kafka.schema.registry.url| the url of kafka schema registry.|
|kafka.schema.record.name| the name of schema record.|
|kafka.schema.record.namespace| the namespace of schema record. |
|kafka.auto.register.schemas| whether to register nonexistent schema automatically.|

### DML 

There are two ways to start a spark streaming query.

- CTAS

```
CREATE TABLE kafka_temp_table
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = "${TOPIC_NAME}",
output.mode = "${OUTPUT_MODE}"",
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = "${SCHEMA_RECORD_NAME}",
kafka.schema.record.namespace = "${SCHEMA_RECORD_NAMESPACE}",
kafka.auto.register.schemas = "${AUTO_REGISTER_SCHEMA_ENABLE}") AS
SELECT
  i_brand_id brand_id,
  i_brand brand,
  sum(ss_ext_sales_price) ext_price
FROM date_dim, kafka_store_sales, item
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 28
  AND d_moy = 11
  AND d_year = 1999
GROUP BY TUMBLING(ss_data_time, interval 1 minute), i_brand, i_brand_id
HAVING delay(ss_data_time) < '2 minutes'
```

- Insert into

```
INSERT INTO kafka_temp_table
SELECT
  i_brand_id brand_id,
  i_brand brand,
  sum(ss_ext_sales_price) ext_price
FROM date_dim, kafka_store_sales, item
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 28
  AND d_moy = 11
  AND d_year = 1999
GROUP BY TUMBLING(ss_data_time, interval 1 minute), i_brand, i_brand_id
HAVING delay(ss_data_time) < '2 minutes'
```

### Queries

Most queries in this testing framework are adapted from [Spark-sql-perf](https://github.com/databricks/spark-sql-perf). 
Besides, we introduced some extended queries which tested new streaming sql syntax.
- Query id less than 99 (including): subset of TPC-DS queries.
- Query id larger than 100 (including): extended queries for new streaming sql syntax testing.

### Extended Streaming SQL syntax

#### window

- HOPPING window: hopping windows model scheduled overlapping windows.

```
Syntax:

GROUP BY HOPPING ( colName, windowDuration, slideDuration ) 

Example:

SELECT avg(inv_quantity_on_hand) qoh
FROM kafka_inventory
GROUP BY HOPPING (inv_data_time, interval 1 minute, interval 30 second)
```

- TUMBLING window: tumbling windows are a series of fixed-sized, non-overlapping and contiguous time intervals.

```
Syntax:

GROUP BY TUMBLING ( colName, windowDuration ) 

Example:

SELECT avg(inv_quantity_on_hand) qoh
FROM kafka_inventory
GROUP BY TUMBLING (inv_data_time, interval 1 minute)
```

#### watermark
In Spark 2.1, we have introduced watermarking, which lets the engine automatically track the current event time in 
the data and attempt to clean up old state accordingly. You can define the watermark of a query by specifying the event 
time column and the threshold on how late the data is expected to be in terms of event time. Here, we introduced a 
built-in function `delay` to express to max delay on event time column.

```
Syntax:

HAVING delay ( colName ) < 'duration' 

Example:

SELECT avg(inv_quantity_on_hand) qoh
FROM kafka_inventory
GROUP BY TUMBLING (inv_data_time, interval 1 minute)
HAVING delay(inv_data_time) < '2 minutes'
```