/*
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
package org.apache.spark.streaming.aliyun.logservice;

import com.aliyun.openservices.log.exception.LogException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

public final class RetryUtil {
  private static final Log LOG = LogFactory.getLog(RetryUtil.class);

  private static final long maxBackoff = 10000;
  private static final long initialBackoff = 1000;
  private static final int maxRetry = 10;

  private static final List<String> UNRECOVERABLE_ERROR_CODES = Arrays.asList(
          "ProjectNotExist",
          "LogStoreNotExist",
          "ConsumerGroupAlreadyExist",
          "ConsumerGroupNotExist",
          "InvalidCursor",
          "Unauthorized"
  );

  static <T> T call(Callable<T> callable) throws Exception {
    long backoff = initialBackoff;
    int retries = 0;
    do {
      try {
        return callable.call();
      } catch (LogException ex) {
        if (UNRECOVERABLE_ERROR_CODES.contains(ex.GetErrorCode())) {
          throw ex;
        }
        if (ex.GetHttpCode() < 500) {
          if (retries >= maxRetry) {
            throw ex;
          }
          ++retries;
        } else {
          retries = 0;
        }
        LOG.warn("Connecting log-service failed: "
            + ex.getMessage() + ", retrying " + retries + "/" + maxRetry);
      } catch (Exception ex) {
        if (retries >= maxRetry) {
          throw ex;
        }
        LOG.warn("Connecting log-service failed: "
            + ex.getMessage() + ", retrying " + retries + "/" + maxRetry);
        ++retries;
      }
      Thread.sleep(backoff);
      backoff = Math.min(backoff * 2, maxBackoff);
    } while (true);
  }
}
