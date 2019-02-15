CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q19;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job19;
SET spark.sql.streaming.checkpointLocation.job19=${CHECKPOINT_ROOT}/job19;

CREATE TABLE kafka_temp_table_q19
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q19',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q19
SELECT
  i_brand_id brand_id,
  i_brand brand,
  i_manufact_id,
  i_manufact,
  sum(ss_ext_sales_price) ext_price
FROM date_dim, kafka_store_sales, item, customer, customer_address, store
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND i_manager_id = 8
  AND d_moy = 11
  AND d_year = 1998
  AND ss_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
  AND ss_store_sk = s_store_sk
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, brand, brand_id, i_manufact_id, i_manufact
