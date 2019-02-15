CREATE DATABASE IF NOT EXISTS ${DB};
USE ${DB};

DROP TABLE IF EXISTS kafka_temp_table_q13;

SET spark.testing=${TESTING_ENABLE};
SET spark.sql.streaming.query.timeout.ms=${TESTING_TIMEOUT_MS};
SET streaming.query.name=job13;
SET spark.sql.streaming.checkpointLocation.job13=${CHECKPOINT_ROOT}/job13;

CREATE TABLE kafka_temp_table_q13
USING kafka OPTIONS (
kafka.bootstrap.servers = "${BOOTSTRAP_SERVERS}",
subscribe = 'temp_topic_q13',
output.mode = 'complete',
kafka.schema.registry.url = "${SCHEMA_REGISTRY_URL}",
kafka.schema.record.name = 'TempResult',
kafka.schema.record.namespace = 'org.apache.spark.emr.baseline.testing',
kafka.auto.register.schemas = 'true');

INSERT INTO kafka_temp_table_q13
SELECT
  avg(ss_quantity),
  avg(ss_ext_sales_price),
  avg(ss_ext_wholesale_cost),
  sum(ss_ext_wholesale_cost)
FROM kafka_store_sales
  , store
  , customer_demographics
  , household_demographics
  , customer_address
  , date_dim
WHERE s_store_sk = ss_store_sk
  AND ss_sold_date_sk = d_date_sk AND d_year = 2001
  AND ((ss_hdemo_sk = hd_demo_sk
  AND cd_demo_sk = ss_cdemo_sk
  AND cd_marital_status = 'M'
  AND cd_education_status = 'Advanced Degree'
  AND ss_sales_price BETWEEN 100.00 AND 150.00
  AND hd_dep_count = 3
) OR
  (ss_hdemo_sk = hd_demo_sk
    AND cd_demo_sk = ss_cdemo_sk
    AND cd_marital_status = 'S'
    AND cd_education_status = 'College'
    AND ss_sales_price BETWEEN 50.00 AND 100.00
    AND hd_dep_count = 1
  ) OR
  (ss_hdemo_sk = hd_demo_sk
    AND cd_demo_sk = ss_cdemo_sk
    AND cd_marital_status = 'W'
    AND cd_education_status = '2 yr Degree'
    AND ss_sales_price BETWEEN 150.00 AND 200.00
    AND hd_dep_count = 1
  ))
  AND ((ss_addr_sk = ca_address_sk
  AND ca_country = 'United States'
  AND ca_state IN ('TX', 'OH', 'TX')
  AND ss_net_profit BETWEEN 100 AND 200
) OR
  (ss_addr_sk = ca_address_sk
    AND ca_country = 'United States'
    AND ca_state IN ('OR', 'NM', 'KY')
    AND ss_net_profit BETWEEN 150 AND 300
  ) OR
  (ss_addr_sk = ca_address_sk
    AND ca_country = 'United States'
    AND ca_state IN ('VA', 'TX', 'MS')
    AND ss_net_profit BETWEEN 50 AND 250
  ));
