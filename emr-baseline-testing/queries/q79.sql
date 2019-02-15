CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q79;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job79;
SET spark.sql.streaming.checkpointLocation.job79=${CHECKPOINT_ROOT}/job79;

CREATE TABLE kafka_temp_table_q79
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q79',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q79
SELECT
  c_last_name,
  c_first_name,
  substr(s_city, 1, 30),
  ss_ticket_number,
  amt,
  profit
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    store.s_city,
    sum(ss_coupon_amt) amt,
    sum(ss_net_profit) profit
  FROM kafka_store_sales, date_dim, store, household_demographics
  WHERE kafka_store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND kafka_store_sales.ss_store_sk = store.s_store_sk
    AND kafka_store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND (household_demographics.hd_dep_count = 6 OR
    household_demographics.hd_vehicle_count > 2)
    AND date_dim.d_dow = 1
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_number_employees BETWEEN 200 AND 295
  GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city) ms, customer
WHERE ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, substr(s_city, 1, 30), profit
