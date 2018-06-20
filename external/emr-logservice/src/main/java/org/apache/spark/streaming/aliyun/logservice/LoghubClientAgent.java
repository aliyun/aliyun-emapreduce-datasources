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
import com.aliyun.openservices.log.http.utils.CodingUtils;
import com.aliyun.openservices.log.request.GetCursorRequest;
import com.aliyun.openservices.log.response.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ConnectTimeoutException;

public class LoghubClientAgent {
  private static final Log LOG = LogFactory.getLog(LoghubClientAgent.class);
  private Client client;
  private int logServiceTimeoutMaxRetry = 3;

  public LoghubClientAgent(String endpoint, String accessId, String accessKey) {
    this.client = new Client(endpoint, accessId, accessKey);
  }

  public ListShardResponse ListShard(String prj, String logStore)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.ListShard(prj, logStore);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public GetCursorResponse GetCursor(String project, String logStream, int shardId, Consts.CursorMode mode)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.GetCursor(project, logStream, shardId, mode);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public GetCursorResponse GetCursor(String project, String logStore, int shardId, long fromTime) throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.GetCursor(project, logStore, shardId, fromTime);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public ConsumerGroupUpdateCheckPointResponse UpdateCheckPoint(String project, String logStore, String consumerGroup,
      int shard, String checkpoint) throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.UpdateCheckPoint(project, logStore, consumerGroup, shard, checkpoint);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public CreateConsumerGroupResponse CreateConsumerGroup(String project, String logStore, ConsumerGroup consumerGroup)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.CreateConsumerGroup(project, logStore, consumerGroup);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public ListConsumerGroupResponse ListConsumerGroup(String project, String logStore) throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.ListConsumerGroup(project, logStore);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public ConsumerGroupCheckPointResponse GetCheckPoint(String project, String logStore, String consumerGroup, int shard)
      throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.GetCheckPoint(project, logStore, consumerGroup, shard);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  public BatchGetLogResponse BatchGetLog(String project, String logStore, int shardId, int count, String cursor,
      String endCursor) throws Exception {
    int retry = 0;
    Exception currentException = null;
    while (retry <= logServiceTimeoutMaxRetry) {
      try {
        return this.client.BatchGetLog(project, logStore, shardId, count, cursor, endCursor);
      } catch (LogException e) {
        if (checkException(e)) {
          retry += 1;
          currentException = e;
        } else {
          throw e;
        }
      }
    }
    LOG.error("reconnect to log-service exceed max retry times[" + logServiceTimeoutMaxRetry + "].");
    assert (currentException != null);
    throw currentException;
  }

  private boolean checkException(LogException e) {
    return e.getCause() != null
        && e.getCause().getCause() != null
        && e.getCause().getCause() instanceof ConnectTimeoutException;
  }
}
