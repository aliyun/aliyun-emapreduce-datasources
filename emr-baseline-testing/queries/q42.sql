CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q42;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job42;
SET spark.sql.streaming.checkpointLocation.job42=${CHECKPOINT_ROOT}/job42;

CREATE TABLE kafka_temp_table_q42
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q42',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q42
SELECT
  dt.d_year,
  item.i_category_id,
  item.i_category,
  sum(ss_ext_sales_price)
FROM date_dim dt, kafka_store_sales, item
WHERE dt.d_date_sk = kafka_store_sales.ss_sold_date_sk
  AND kafka_store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manager_id = 1
  AND dt.d_moy = 11
  AND dt.d_year = 2000
GROUP BY dt.d_year
  , item.i_category_id
  , item.i_category
ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year
  , item.i_category_id
  , item.i_category
