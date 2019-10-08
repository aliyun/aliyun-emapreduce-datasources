package org.apache.spark.streaming.aliyun.logservice.constants;

import org.apache.spark.streaming.aliyun.logservice.utils.VersionInfoUtils;

/**
 * @author jiashuangkai
 * @version 1.0
 * @since 2019-09-27 14:56
 */
public interface Constants {
    String LOG_CONNECTOR_USER_AGENT = VersionInfoUtils.getDefaultUserAgent();
}
