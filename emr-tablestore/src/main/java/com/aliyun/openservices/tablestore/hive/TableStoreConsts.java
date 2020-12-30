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

package com.aliyun.openservices.tablestore.hive;

public class TableStoreConsts {
    public static final String ENDPOINT = "tablestore.endpoint";
    public static final String INSTANCE = "tablestore.instance";
    public static final String TABLE_NAME = "tablestore.table.name";
    public static final String ACCESS_KEY_ID = "tablestore.access_key_id";
    public static final String ACCESS_KEY_SECRET = "tablestore.access_key_secret";
    public static final String SECURITY_TOKEN = "tablestore.security_token";
    public static final String MAX_UPDATE_BATCH_SIZE = "tablestore.max_update_batch_size";
    public static final String COMPUTE_MODE = "tablestore.compute.mode";
    public static final String MAX_SPLIT_COUNT = "tablestore.max.split.count";
    public static final String SPLIT_SIZE_MBS = "tablestore.split.size.mbs";

    public static final String COLUMNS_MAPPING = "tablestore.columns.mapping";
    public static final String FILTER = "filters";

    public static final  String PROPERTIES_FILE_PATH = "tablestore.properties.path";

    public static final String[] REQUIRES = new String[] {
        ENDPOINT,
        TABLE_NAME,
        ACCESS_KEY_ID,
        ACCESS_KEY_SECRET};
    public static final String[] OPTIONALS = new String[] {
        INSTANCE,
        SECURITY_TOKEN,
        MAX_UPDATE_BATCH_SIZE,
        COLUMNS_MAPPING};
}
