package com.aliyun.openservices.tablestore.hive;

public class TableStoreConsts {
    final public static String ENDPOINT = "tablestore.endpoint";
    final public static String INSTANCE = "tablestore.instance";
    final public static String TABLE_NAME = "tablestore.table.name";
    final public static String ACCESS_KEY_ID = "tablestore.access_key_id";
    final public static String ACCESS_KEY_SECRET = "tablestore.access_key_secret";
    final public static String STS_TOKEN = "tablestore.sts_token";

    final public static String COLUMNS_MAPPING = "tablestore.columns.mapping";

    final public static String[] REQUIRES = new String[] {
        ENDPOINT,
        TABLE_NAME,
        ACCESS_KEY_ID,
        ACCESS_KEY_SECRET};
    final public static String[] OPTIONALS = new String[] {
        INSTANCE,
        STS_TOKEN,
        COLUMNS_MAPPING};
}
