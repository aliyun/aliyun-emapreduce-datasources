CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q96;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job96;
SET spark.sql.streaming.checkpointLocation.job96=${CHECKPOINT_ROOT}/job96;

CREATE TABLE kafka_temp_table_q96
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q96',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q96
SELECT count(*)
FROM kafka_store_sales, household_demographics, time_dim, store
WHERE ss_sold_time_sk = time_dim.t_time_sk
  AND ss_hdemo_sk = household_demographics.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND time_dim.t_hour = 20
  AND time_dim.t_minute >= 30
  AND household_demographics.hd_dep_count = 7
  AND store.s_store_name = 'ese'
ORDER BY count(*)
