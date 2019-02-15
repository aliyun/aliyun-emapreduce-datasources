CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q62;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job62;
SET spark.sql.streaming.checkpointLocation.job62=${CHECKPOINT_ROOT}/job62;

CREATE TABLE kafka_temp_table_q62
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q62',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q62
SELECT
  substr(w_warehouse_name, 1, 20),
  sm_type,
  web_name,
  sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30)
    THEN 1
      ELSE 0 END)  AS `30 days `,
  sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 30) AND
    (ws_ship_date_sk - ws_sold_date_sk <= 60)
    THEN 1
      ELSE 0 END)  AS `31 - 60 days `,
  sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 60) AND
    (ws_ship_date_sk - ws_sold_date_sk <= 90)
    THEN 1
      ELSE 0 END)  AS `61 - 90 days `,
  sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 90) AND
    (ws_ship_date_sk - ws_sold_date_sk <= 120)
    THEN 1
      ELSE 0 END)  AS `91 - 120 days `,
  sum(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120)
    THEN 1
      ELSE 0 END)  AS `>120 days `
FROM
  kafka_web_sales, warehouse, ship_mode, web_site, date_dim
WHERE
  d_month_seq BETWEEN 1200 AND 1200 + 11
    AND ws_ship_date_sk = d_date_sk
    AND ws_warehouse_sk = w_warehouse_sk
    AND ws_ship_mode_sk = sm_ship_mode_sk
    AND ws_web_site_sk = web_site_sk
GROUP BY
  substr(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY
  substr(w_warehouse_name, 1, 20), sm_type, web_name
