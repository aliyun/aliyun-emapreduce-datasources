CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q15;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job15;
SET spark.sql.streaming.checkpointLocation.job15=${CHECKPOINT_ROOT}/job15;

CREATE TABLE kafka_temp_table_q15
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q15',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q15
SELECT
  ca_zip,
  sum(cs_sales_price)
FROM kafka_catalog_sales, customer, customer_address, date_dim
WHERE cs_bill_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND (substr(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475',
                                '85392', '85460', '80348', '81792')
  OR ca_state IN ('CA', 'WA', 'GA')
  OR cs_sales_price > 500)
  AND cs_sold_date_sk = d_date_sk
  AND d_qoy = 2 AND d_year = 2001
GROUP BY ca_zip
ORDER BY ca_zip;
