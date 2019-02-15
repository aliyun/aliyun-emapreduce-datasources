CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q76;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job76;
SET spark.sql.streaming.checkpointLocation.job76=${CHECKPOINT_ROOT}/job76;

CREATE TABLE kafka_temp_table_q76
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q76',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q76
SELECT
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) sales_cnt,
  SUM(ext_sales_price) sales_amt
FROM (
       SELECT
         'store' AS channel,
         ss_store_sk col_name,
         d_year,
         d_qoy,
         i_category,
         ss_ext_sales_price ext_sales_price
       FROM kafka_store_sales, item, date_dim
       WHERE ss_store_sk IS NULL
         AND ss_sold_date_sk = d_date_sk
         AND ss_item_sk = i_item_sk
       UNION ALL
       SELECT
         'web' AS channel,
         ws_ship_customer_sk col_name,
         d_year,
         d_qoy,
         i_category,
         ws_ext_sales_price ext_sales_price
       FROM kafka_web_sales, item, date_dim
       WHERE ws_ship_customer_sk IS NULL
         AND ws_sold_date_sk = d_date_sk
         AND ws_item_sk = i_item_sk
       UNION ALL
       SELECT
         'catalog' AS channel,
         cs_ship_addr_sk col_name,
         d_year,
         d_qoy,
         i_category,
         cs_ext_sales_price ext_sales_price
       FROM kafka_catalog_sales, item, date_dim
       WHERE cs_ship_addr_sk IS NULL
         AND cs_sold_date_sk = d_date_sk
         AND cs_item_sk = i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
