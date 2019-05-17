CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q40;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job40;
SET spark.sql.streaming.checkpointLocation.job40=${CHECKPOINT_ROOT}/job40;

CREATE TABLE kafka_temp_table_q40
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q40',
output.mode = 'append',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q40
SELECT
  w_state,
  i_item_id
FROM
  kafka_catalog_sales
  LEFT OUTER JOIN kafka_catalog_returns ON
                                    (cs_order_number = cr_order_number
                                      AND cs_item_sk = cr_item_sk
                                      AND cs_data_time >= cr_data_time
                                      AND cs_data_time <= cr_data_time + interval 30 seconds)
  , warehouse, item, date_dim
WHERE
  i_current_price BETWEEN 0.99 AND 1.49
    AND i_item_sk = cs_item_sk
    AND cs_warehouse_sk = w_warehouse_sk
    AND cs_sold_date_sk = d_date_sk
    AND d_date BETWEEN (cast('2000-03-11' AS DATE) - INTERVAL 30 days)
  AND (cast('2000-03-11' AS DATE) + INTERVAL 30 days)
  AND delay(cr_data_time) < '30 seconds' and delay(cs_data_time) < '60 seconds'
