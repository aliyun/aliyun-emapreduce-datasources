CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q71;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job71;
SET spark.sql.streaming.checkpointLocation.job71=${CHECKPOINT_ROOT}/job71;

CREATE TABLE kafka_temp_table_q71
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q71',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q71
SELECT
  i_brand_id brand_id,
  i_brand brand,
  t_hour,
  t_minute,
  sum(ext_price) ext_price
FROM item,
  (SELECT
     ws_ext_sales_price AS ext_price,
     ws_sold_date_sk AS sold_date_sk,
     ws_item_sk AS sold_item_sk,
     ws_sold_time_sk AS time_sk
   FROM kafka_web_sales, date_dim
   WHERE d_date_sk = ws_sold_date_sk
     AND d_moy = 11
     AND d_year = 1999
   UNION ALL
   SELECT
     cs_ext_sales_price AS ext_price,
     cs_sold_date_sk AS sold_date_sk,
     cs_item_sk AS sold_item_sk,
     cs_sold_time_sk AS time_sk
   FROM kafka_catalog_sales, date_dim
   WHERE d_date_sk = cs_sold_date_sk
     AND d_moy = 11
     AND d_year = 1999
   UNION ALL
   SELECT
     ss_ext_sales_price AS ext_price,
     ss_sold_date_sk AS sold_date_sk,
     ss_item_sk AS sold_item_sk,
     ss_sold_time_sk AS time_sk
   FROM kafka_store_sales, date_dim
   WHERE d_date_sk = ss_sold_date_sk
     AND d_moy = 11
     AND d_year = 1999
  ) AS tmp, time_dim
WHERE
  sold_item_sk = i_item_sk
    AND i_manager_id = 1
    AND time_sk = t_time_sk
    AND (t_meal_time = 'breakfast' OR t_meal_time = 'dinner')
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, brand_id
