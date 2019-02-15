CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q27;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job27;
SET spark.sql.streaming.checkpointLocation.job27=${CHECKPOINT_ROOT}/job27;

CREATE TABLE kafka_temp_table_q27
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q27',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q27
SELECT
  i_item_id,
  s_state,
  grouping(s_state) g_state,
  avg(ss_quantity) agg1,
  avg(ss_list_price) agg2,
  avg(ss_coupon_amt) agg3,
  avg(ss_sales_price) agg4
FROM kafka_store_sales, customer_demographics, date_dim, store, item
WHERE ss_sold_date_sk = d_date_sk AND
  ss_item_sk = i_item_sk AND
  ss_store_sk = s_store_sk AND
  ss_cdemo_sk = cd_demo_sk AND
  cd_gender = 'M' AND
  cd_marital_status = 'S' AND
  cd_education_status = 'College' AND
  d_year = 2002 AND
  s_state IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN')
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id, s_state
