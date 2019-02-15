CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q18;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job18;
SET spark.sql.streaming.checkpointLocation.job18=${CHECKPOINT_ROOT}/job18;

CREATE TABLE kafka_temp_table_q18
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q18',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q18
SELECT
  i_item_id,
  ca_country,
  ca_state,
  ca_county,
  avg(cast(cs_quantity AS DECIMAL(12, 2))) agg1,
  avg(cast(cs_list_price AS DECIMAL(12, 2))) agg2,
  avg(cast(cs_coupon_amt AS DECIMAL(12, 2))) agg3,
  avg(cast(cs_sales_price AS DECIMAL(12, 2))) agg4,
  avg(cast(cs_net_profit AS DECIMAL(12, 2))) agg5,
  avg(cast(c_birth_year AS DECIMAL(12, 2))) agg6,
  avg(cast(cd1.cd_dep_count AS DECIMAL(12, 2))) agg7
FROM kafka_catalog_sales, customer_demographics cd1,
  customer_demographics cd2, customer, customer_address, date_dim, item
WHERE cs_sold_date_sk = d_date_sk AND
  cs_item_sk = i_item_sk AND
  cs_bill_cdemo_sk = cd1.cd_demo_sk AND
  cs_bill_customer_sk = c_customer_sk AND
  cd1.cd_gender = 'F' AND
  cd1.cd_education_status = 'Unknown' AND
  c_current_cdemo_sk = cd2.cd_demo_sk AND
  c_current_addr_sk = ca_address_sk AND
  c_birth_month IN (1, 6, 8, 9, 12, 2) AND
  d_year = 1998 AND
  ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS')
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
