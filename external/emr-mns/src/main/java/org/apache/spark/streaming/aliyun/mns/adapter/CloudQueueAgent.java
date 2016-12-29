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
package org.apache.spark.streaming.aliyun.mns.adapter;

import com.aliyun.mns.model.Message;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.List;

public class CloudQueueAgent {
  private Object cloudQueue;
  private Class cloudQueueClz;
  private String endpoint;
  private String queueName;
  private Gson gson = new Gson();
  private URLClassLoader urlClassLoader;

  public CloudQueueAgent(Object cloudQueue, Class cloudQueueClz,
      String endpoint, String queueName, URLClassLoader classLoader) {
    this.cloudQueue = cloudQueue;
    this.cloudQueueClz = cloudQueueClz;
    this.endpoint = endpoint;
    this.queueName = queueName;
    this.urlClassLoader = classLoader;
  }

  @SuppressWarnings("unchecked")
  public List<Message> batchPopMessage(int batchMsgSize, int pollingWaitSeconds,
      boolean retry) throws Exception {
    try {
      Method method =
          cloudQueueClz.getMethod("batchPopMessage", Integer.TYPE, Integer.TYPE);
      Object ret = method.invoke(cloudQueue, batchMsgSize, pollingWaitSeconds);
      return gson.fromJson(gson.toJson(ret), new TypeToken<List<Message>>() {
      }.getType());
    } catch (Exception e) {
      if (retry) {
        Object mnsClient = MNSAgentUtil.updateMNSClient(e,
            urlClassLoader, endpoint);
        if (mnsClient != null) {
          Class mnsClientClz = urlClassLoader
              .loadClass("com.aliyun.mns.client.MNSClient");
          Method method = mnsClientClz.getMethod("getQueueRef", String.class);
          cloudQueue = method.invoke(mnsClient, queueName);
          return batchPopMessage(batchMsgSize, pollingWaitSeconds, false);
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<Message> batchPopMessage(int batchMsgSize, int pollingWaitSeconds)
      throws Exception {
    return batchPopMessage(batchMsgSize, pollingWaitSeconds, true);
  }

  @SuppressWarnings("unchecked")
  public void batchDeleteMessage(List<String> receiptsToDelete, boolean retry)
      throws Exception {
    try {
      Method method = cloudQueueClz.getMethod("batchDeleteMessage", List.class);
      method.invoke(cloudQueue, receiptsToDelete);
    } catch (Exception e) {
      if (retry) {
        Object mnsClient = MNSAgentUtil.updateMNSClient(e,
            urlClassLoader, endpoint);
        if (mnsClient != null) {
          Class mnsClientClz = urlClassLoader
              .loadClass("com.aliyun.mns.client.MNSClient");
          Method method = mnsClientClz.getMethod("getQueueRef", String.class);
          cloudQueue = method.invoke(mnsClient, queueName);
          batchDeleteMessage(receiptsToDelete, false);
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void batchDeleteMessage(List<String> receiptsToDelete)
      throws Exception {
    batchDeleteMessage(receiptsToDelete, true);
  }
}
