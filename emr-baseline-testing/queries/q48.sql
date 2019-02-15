CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q48;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job48;
SET spark.sql.streaming.checkpointLocation.job48=${CHECKPOINT_ROOT}/job48;

CREATE TABLE kafka_temp_table_q48
USING kafka
OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q48',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q48
SELECT sum(ss_quantity)
FROM kafka_store_sales, store, customer_demographics, customer_address, date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk AND d_year = 2001
  AND
  (
    (
      cd_demo_sk = ss_cdemo_sk
        AND
        cd_marital_status = 'M'
        AND
        cd_education_status = '4 yr Degree'
        AND
        ss_sales_price BETWEEN 100.00 AND 150.00
    )
      OR
      (
        cd_demo_sk = ss_cdemo_sk
          AND
          cd_marital_status = 'D'
          AND
          cd_education_status = '2 yr Degree'
          AND
          ss_sales_price BETWEEN 50.00 AND 100.00
      )
      OR
      (
        cd_demo_sk = ss_cdemo_sk
          AND
          cd_marital_status = 'S'
          AND
          cd_education_status = 'College'
          AND
          ss_sales_price BETWEEN 150.00 AND 200.00
      )
  )
  AND
  (
    (
      ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('CO', 'OH', 'TX')
        AND ss_net_profit BETWEEN 0 AND 2000
    )
      OR
      (ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('OR', 'MN', 'KY')
        AND ss_net_profit BETWEEN 150 AND 3000
      )
      OR
      (ss_addr_sk = ca_address_sk
        AND
        ca_country = 'United States'
        AND
        ca_state IN ('VA', 'CA', 'MS')
        AND ss_net_profit BETWEEN 50 AND 25000
      )
  )
