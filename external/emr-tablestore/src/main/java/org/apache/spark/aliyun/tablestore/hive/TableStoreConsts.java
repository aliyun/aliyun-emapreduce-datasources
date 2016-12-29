/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.aliyun.tablestore.hive;

public class TableStoreConsts {
    final public static String ENDPOINT = "tablestore.endpoint";
    final public static String INSTANCE = "tablestore.instance";
    final public static String TABLE_NAME = "tablestore.table.name";
    final public static String ACCESS_KEY_ID = "tablestore.access_key_id";
    final public static String ACCESS_KEY_SECRET = "tablestore.access_key_secret";
    final public static String SECURITY_TOKEN = "tablestore.security_token";

    final public static String COLUMNS_MAPPING = "tablestore.columns.mapping";

    final public static String[] REQUIRES = new String[] {
        ENDPOINT,
        TABLE_NAME,
        ACCESS_KEY_ID,
        ACCESS_KEY_SECRET};
    final public static String[] OPTIONALS = new String[] {
        INSTANCE,
        SECURITY_TOKEN,
        COLUMNS_MAPPING};
}
