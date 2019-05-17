CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q102;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job102;
SET spark.sql.streaming.checkpointLocation.job102=${CHECKPOINT_ROOT}/job102;

CREATE TABLE kafka_temp_table_q102
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q102',
output.mode = 'append',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q102
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
  AND delay(ss_data_time) < '2 minutes'
GROUP BY TUMBLING(ss_data_time, interval 1 minute), i_brand, i_brand_id
