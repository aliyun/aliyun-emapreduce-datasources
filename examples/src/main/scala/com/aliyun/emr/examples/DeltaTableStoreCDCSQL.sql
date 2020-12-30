/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- streaming-sql --master yarn --use-emr-datasource

DROP TABLE IF EXISTS order_source;
CREATE TABLE order_source
USING tablestore
OPTIONS(
endpoint="http://<InstanceName>.cn-hangzhou.vpc.tablestore.aliyuncs.com",
access.key.id="",
access.key.secret="",
instance.name="<InstanceName>",
table.name="OrderSource",
catalog='{"columns": {"UserId": {"type": "string"}, "OrderId": {"type": "string"},"price": {"type": "double"}, "timestamp": {"type": "long"}}}'
);

DROP TABLE IF EXISTS delta_orders;
CREATE TABLE delta_orders(
UserId string,
OrderId string,
price double,
timestamp long
)
USING delta
LOCATION '/delta/orders';

CREATE SCAN incremental_orders ON order_source USING STREAM
OPTIONS(
tunnel.id="07bb3bfc-4bb6-4285-b3d6-c7b77f36bc49",
maxoffsetsperchannel="10000");

-- create temporary function ots_col_parser as 'org.apache.spark.sql.aliyun.udfs.tablestore.ResolveTableStoreBinlogUDF' using jar '/root/emr-sql_2.11-2.2.0.jar';
CREATE STREAM orders_job
OPTIONS (
checkpointLocation='/delta/orders_checkpoint',
triggerIntervalMs='3000'
)
MERGE INTO delta_orders
USING (
  SELECT __ots_record_type__ AS RecordType, __ots_record_timestamp__ AS RecordTimestamp,
  ots_col_parser(UserId, __ots_column_type_UserId) AS UserId,
  ots_col_parser(OrderId, __ots_column_type_OrderId) AS OrderId,
  ots_col_parser(price, __ots_column_type_price) AS price,
  ots_col_parser(timestamp, __ots_column_type_timestamp) AS timestamp
  FROM incremental_orders
) AS delta_source
ON delta_orders.UserId=delta_source.UserId AND delta_orders.OrderId=delta_source.OrderId
WHEN MATCHED AND delta_source.RecordType='DELETE' THEN
DELETE
WHEN MATCHED AND delta_source.RecordType='UPDATE' THEN
UPDATE SET UserId=delta_source.UserId, OrderId=delta_source.OrderId, price=delta_source.price, timestamp=delta_source.timestamp
WHEN NOT MATCHED AND delta_source.RecordType='PUT' THEN
INSERT (UserId, OrderId, price, timestamp) values (delta_source.UserId, delta_source.OrderId, delta_source.price, delta_source.timestamp);

