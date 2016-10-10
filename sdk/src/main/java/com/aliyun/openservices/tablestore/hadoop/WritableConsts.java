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

package com.aliyun.openservices.tablestore.hadoop;

public class WritableConsts {
    public static final byte DATATYPE_INF_MIN = 3;
    public static final byte DATATYPE_INF_MAX = 5;
    public static final byte DATATYPE_STR = 6;
    public static final byte DATATYPE_INT = 9;
    public static final byte DATATYPE_BIN = 10;
    public static final byte DATATYPE_DBL = 12;
    public static final byte DATATYPE_BOOL = 15;
    
    public static final byte PRIMARY_KEY = 17;
    public static final byte PRIMARY_KEY_COLUMN = 18;
    public static final byte COLUMN_VALUE = 20;
    public static final byte COLUMN_WITHOUT_TIMESTAMP = 23;
    public static final byte COLUMN_WITH_TIMESTAMP = 24;
    public static final byte ROW = 27;

    public static final byte GETRANGE_ROW_QUERY_CRITERIA = 29;
    public static final byte GETRANGE_COLUMNS_TO_GET = 33;
    public static final byte GETRANGE_MAX_VERSIONS = 34;
    public static final byte GETRANGE_TIME_RANGE = 36;
    public static final byte GETRANGE_CACHE_BLOCKS = 39;
    public static final byte GETRANGE_START_COLUMN = 40;
    public static final byte GETRANGE_END_COLUMN = 43;
    public static final byte GETRANGE_LIMIT = 45;
    public static final byte GETRANGE_BACKWARDS = 46;
    public static final byte GETRANGE_START_PKEY = 48;
    public static final byte GETRANGE_END_PKEY = 51;

    public static final byte CREDENTIAL = 53;
    public static final byte CREDENTIAL_SECURITY_TOKEN = 54;
    public static final byte ENDPOINT = 57;
    public static final byte MULTI_CRITERIA = 58;

    public static final byte FILTER = 60;
    public static final byte FILTER_COLUMN_PAGINATION = 63;
    public static final byte FILTER_SINGLE_COLUMN = 65;
    public static final byte FILTER_EQUAL = 66;
    public static final byte FILTER_NOT_EQUAL = 68;
    public static final byte FILTER_GREATER = 71;
    public static final byte FILTER_GREATER_EQUAL = 72;
    public static final byte FILTER_LESS = 75;
    public static final byte FILTER_LESS_EQUAL = 77;
    public static final byte FILTER_COMPOSITED = 78;
    public static final byte FILTER_NOT = 80;
    public static final byte FILTER_AND = 83;
    public static final byte FILTER_OR = 85;
    // public static final byte x = 86;
    // public static final byte x = 89;
    // public static final byte x = 90;
    // public static final byte x = 92;
    // public static final byte x = 95;
    // public static final byte x = 96;
    // public static final byte x = 99;
    // public static final byte x = 101;
    // public static final byte x = 102;
    // public static final byte x = 105;
    // public static final byte x = 106;
    // public static final byte x = 108;
    // public static final byte x = 111;
    // public static final byte x = 113;
    // public static final byte x = 114;
    // public static final byte x = 116;
    // public static final byte x = 119;
    // public static final byte x = 120;
    // public static final byte x = 123;
    // public static final byte x = 125;
    // public static final byte x = 126;
    // public static final byte x = 129;
    // public static final byte x = 130;
    // public static final byte x = 132;
    // public static final byte x = 135;
    // public static final byte x = 136;
    // public static final byte x = 139;
    // public static final byte x = 141;
    // public static final byte x = 142;
    // public static final byte x = 144;
    // public static final byte x = 147;
    // public static final byte x = 149;
    // public static final byte x = 150;
    // public static final byte x = 153;
    // public static final byte x = 154;
    // public static final byte x = 156;
    // public static final byte x = 159;
    // public static final byte x = 160;
    // public static final byte x = 163;
    // public static final byte x = 165;
    // public static final byte x = 166;
    // public static final byte x = 169;
    // public static final byte x = 170;
    // public static final byte x = 172;
    // public static final byte x = 175;
    // public static final byte x = 177;
    // public static final byte x = 178;
    // public static final byte x = 180;
    // public static final byte x = 183;
    // public static final byte x = 184;
    // public static final byte x = 187;
    // public static final byte x = 189;
    // public static final byte x = 190;
    // public static final byte x = 192;
    // public static final byte x = 195;
    // public static final byte x = 197;
    // public static final byte x = 198;
    // public static final byte x = 201;
    // public static final byte x = 202;
    // public static final byte x = 204;
    // public static final byte x = 207;
    // public static final byte x = 209;
    // public static final byte x = 210;
    // public static final byte x = 212;
    // public static final byte x = 215;
    // public static final byte x = 216;
    // public static final byte x = 219;
    // public static final byte x = 221;
    // public static final byte x = 222;
    // public static final byte x = 225;
    // public static final byte x = 226;
    // public static final byte x = 228;
    // public static final byte x = 231;
    // public static final byte x = 232;
    // public static final byte x = 235;
    // public static final byte x = 237;
    // public static final byte x = 238;
    // public static final byte x = 240;
    // public static final byte x = 243;
    // public static final byte x = 245;
    // public static final byte x = 246;
    // public static final byte x = 249;
    // public static final byte x = 250;
    // public static final byte x = 252;
    // public static final byte x = 255;
}

