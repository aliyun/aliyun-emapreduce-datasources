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
package org.apache.spark.streaming.aliyun.logservice;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ConnectTimeoutException;

public class LoghubClientAgent {
  private static final Log LOG = LogFactory.getLog(LoghubClientAgent.class);
  private Client client;
  private int maxRetry = 6;
  private long initialBackoff = 1000;
  private long maxBackoff = 10000;

  public LoghubClientAgent(String endpoint, String accessId, String accessKey) {
    this.client = new Client(endpoint, accessId, accessKey);
  }

  public ListShardResponse ListShard(String logProject, String logStore)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.ListShard(logProject, logStore);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public GetCursorResponse GetCursor(String project, String logStream, int shardId, Consts.CursorMode mode)
      throws Exception {
    int retry = 0;
    long backoff = initialBackoff;
    Exception currentException = null;
    while (retry <= maxRetry) {
      try {
        return this.client.GetCursor(project, logStream, shardId, mode);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public GetCursorResponse GetCursor(String project, String logStore, int shardId, long fromTime) throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.GetCursor(project, logStore, shardId, fromTime);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public ConsumerGroupUpdateCheckPointResponse UpdateCheckPoint(String project, String logStore, String consumerGroup,
      int shard, String checkpoint) throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.UpdateCheckPoint(project, logStore, consumerGroup, shard, checkpoint);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public CreateConsumerGroupResponse CreateConsumerGroup(String project, String logStore, ConsumerGroup consumerGroup)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.CreateConsumerGroup(project, logStore, consumerGroup);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public ListConsumerGroupResponse ListConsumerGroup(String project, String logStore) throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.ListConsumerGroup(project, logStore);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public ConsumerGroupCheckPointResponse GetCheckPoint(String project, String logStore, String consumerGroup, int shard)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.GetCheckPoint(project, logStore, consumerGroup, shard);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public BatchGetLogResponse BatchGetLog(String project, String logStore, int shardId, int count, String cursor,
      String endCursor) throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.BatchGetLog(project, logStore, shardId, count, cursor, endCursor);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public GetHistogramsResponse GetHistograms(String project, String logStore, int from, int to, String topic,
      String query) throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.GetHistograms(project, logStore, from, to, topic, query);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          if (e.GetErrorCode().equals("IndexConfigNotExist")) {
            LOG.warn("Please enable index service for " + project + "/" + logStore);
          }
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public GetCursorTimeResponse GetCursorTime(String project, String logStore, int shardId, String cursor)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    long backoff = initialBackoff;
    while (retry <= maxRetry) {
      try {
        return this.client.GetCursorTime(project, logStore, shardId, cursor);
      } catch (LogException e) {
        if (shouldRetry(e, retry)) {
          retry += 1;
          Thread.sleep(backoff);
          backoff = Math.min(backoff * 2, maxBackoff);
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + maxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  private static boolean checkConnectionTimeoutException(LogException e) {
    return e.getCause() != null
        && e.getCause().getCause() != null
        && e.getCause().getCause() instanceof ConnectTimeoutException;
  }

  private static boolean isRecoverableException(LogException ex) {
    return checkConnectionTimeoutException(ex)
            || ex.GetHttpCode() >= 500 // Internal server error
            || ex.GetHttpCode() == 403; // Request rate exceed quota
  }

  private boolean shouldRetry(LogException ex, int retry) {
    return isRecoverableException(ex) && retry < maxRetry;
  }
}
