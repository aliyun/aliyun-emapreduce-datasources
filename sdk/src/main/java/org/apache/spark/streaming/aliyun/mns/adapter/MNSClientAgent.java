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

import com.google.gson.Gson;

import java.lang.reflect.Method;
import java.net.URLClassLoader;

public class MNSClientAgent {
  private static Gson gson = new Gson();
  private Class mnsClientClz;
  private Object mnsClient;
  private String endpoint;
  private URLClassLoader urlClassLoader;

  public MNSClientAgent(Object mnsClient, Class mnsClientClz,
      String endpoint, URLClassLoader classLoader) {
    this.mnsClientClz = mnsClientClz;
    this.mnsClient = mnsClient;
    this.endpoint = endpoint;
    this.urlClassLoader = classLoader;
  }

  @SuppressWarnings("unchecked")
  public CloudQueueAgent getQueueRef(String queueName, boolean retry)
      throws Exception {
    try {
      Method method = mnsClientClz.getMethod("getQueueRef", String.class);
      Object cloudQueue = method.invoke(mnsClient, queueName);
      Class cloudQueueClz =
          urlClassLoader.loadClass("com.aliyun.mns.client.CloudQueue");
      return new CloudQueueAgent(cloudQueue, cloudQueueClz, endpoint, queueName,
          urlClassLoader);
    } catch (Exception e) {
      if (retry) {
        Object mnsClient = MNSAgentUtil.updateMNSClient(e,
            urlClassLoader, endpoint);
        if (mnsClient != null) {
          this.mnsClient = mnsClient;
          return getQueueRef(queueName, false);
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public CloudQueueAgent getQueueRef(String queueName) throws Exception {
    return getQueueRef(queueName, true);
  }

  @SuppressWarnings("unchecked")
  public boolean isOpen(boolean retry) throws Exception {
    try {
      Method method = mnsClientClz.getMethod("isOpen");
      Object ret = method.invoke(mnsClient);
      return gson.fromJson(gson.toJson(ret), Boolean.class);
    } catch (Exception e) {
      if (retry) {
        Object mnsClient = MNSAgentUtil.updateMNSClient(e,
            urlClassLoader, endpoint);
        if (mnsClient != null) {
          this.mnsClient = mnsClient;
          return isOpen(false);
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public boolean isOpen() throws Exception {
    return isOpen(true);
  }

  @SuppressWarnings("unchecked")
  public synchronized void close(boolean retry) throws Exception {
    try {
      Method method = mnsClientClz.getMethod("close");
      method.invoke(mnsClient);
    } catch (Exception e) {
      if (retry) {
        Object mnsClient = MNSAgentUtil.updateMNSClient(e,
            urlClassLoader, endpoint);
        if (mnsClient != null) {
          this.mnsClient = mnsClient;
          close(false);
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void close() throws Exception {
    close(true);
  }
}
