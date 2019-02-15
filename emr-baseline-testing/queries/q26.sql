CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q26;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job26;
SET spark.sql.streaming.checkpointLocation.job26=${CHECKPOINT_ROOT}/job26;

CREATE TABLE kafka_temp_table_q26
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q26',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q26
SELECT
  i_item_id,
  avg(cs_quantity) agg1,
  avg(cs_list_price) agg2,
  avg(cs_coupon_amt) agg3,
  avg(cs_sales_price) agg4
FROM kafka_catalog_sales, customer_demographics, date_dim, item, promotion
WHERE cs_sold_date_sk = d_date_sk AND
  cs_item_sk = i_item_sk AND
  cs_bill_cdemo_sk = cd_demo_sk AND
  cs_promo_sk = p_promo_sk AND
  cd_gender = 'M' AND
  cd_marital_status = 'S' AND
  cd_education_status = 'College' AND
  (p_channel_email = 'N' OR p_channel_event = 'N') AND
  d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
