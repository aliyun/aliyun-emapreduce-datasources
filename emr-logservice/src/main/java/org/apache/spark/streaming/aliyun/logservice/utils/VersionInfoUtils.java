package org.apache.spark.streaming.aliyun.logservice.utils;

import java.io.InputStream;
import java.util.Properties;

public class VersionInfoUtils {
    private static final String VERSION_INFO_FILE = "spark-versioninfo.properties";
    private static final String USER_AGENT_PREFIX = "aliyun-emapreduce-sdk";

    private static String version = null;

    private static String defaultUserAgent = null;

    public static String getVersion() {
        if (version == null) {
            initializeVersion();
        }
        return version;
    }

    public static String getDefaultUserAgent() {
        if (defaultUserAgent == null) {
            defaultUserAgent = USER_AGENT_PREFIX + "-" + getVersion()+"/" + System.getProperty("java.version");
        }
        return defaultUserAgent;
    }

    private static void initializeVersion() {
        InputStream inputStream = VersionInfoUtils.class.getClassLoader().getResourceAsStream(VERSION_INFO_FILE);
        Properties versionInfoProperties = new Properties();
        try {
            if (inputStream == null) {
                throw new IllegalArgumentException(VERSION_INFO_FILE + " not found on classpath");
            }
            versionInfoProperties.load(inputStream);
            version = versionInfoProperties.getProperty("version");
        } catch (Exception e) {
            version = "unknown-version";
        }
    }
}
