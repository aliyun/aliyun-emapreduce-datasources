CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q46;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job46;
SET spark.sql.streaming.checkpointLocation.job46=${CHECKPOINT_ROOT}/job46;

CREATE TABLE kafka_temp_table_q46
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q46',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q46
SELECT
  c_last_name,
  c_first_name,
  ca_city,
  bought_city,
  ss_ticket_number,
  amt,
  profit
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    ca_city bought_city,
    sum(ss_coupon_amt) amt,
    sum(ss_net_profit) profit
  FROM kafka_store_sales, date_dim, store, household_demographics, customer_address
  WHERE kafka_store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND kafka_store_sales.ss_store_sk = store.s_store_sk
    AND kafka_store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND kafka_store_sales.ss_addr_sk = customer_address.ca_address_sk
    AND (household_demographics.hd_dep_count = 4 OR
    household_demographics.hd_vehicle_count = 3)
    AND date_dim.d_dow IN (6, 0)
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_city IN ('Fairview', 'Midway', 'Fairview', 'Fairview', 'Fairview')
  GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city) dn, customer,
  customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
  AND customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number
