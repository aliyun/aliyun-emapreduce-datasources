CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q68;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job68;
SET spark.sql.streaming.checkpointLocation.job68=${CHECKPOINT_ROOT}/job68;

CREATE TABLE kafka_temp_table_q68
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q68',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q68
SELECT
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  extended_price,
  extended_tax,
  list_price
FROM (SELECT
  ss_ticket_number,
  ss_customer_sk,
  ca_city bought_city,
  sum(ss_ext_sales_price) extended_price,
  sum(ss_ext_list_price) list_price,
  sum(ss_ext_tax) extended_tax
FROM kafka_store_sales, date_dim, store, household_demographics, customer_address
WHERE kafka_store_sales.ss_sold_date_sk = date_dim.d_date_sk
  AND kafka_store_sales.ss_store_sk = store.s_store_sk
  AND kafka_store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
  AND kafka_store_sales.ss_addr_sk = customer_address.ca_address_sk
  AND date_dim.d_dom BETWEEN 1 AND 2
  AND (household_demographics.hd_dep_count = 4 OR
  household_demographics.hd_vehicle_count = 3)
  AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
  AND store.s_city IN ('Midway', 'Fairview')
GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) dn,
  customer,
  customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, ss_ticket_number
