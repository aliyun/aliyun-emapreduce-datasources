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

import com.aliyun.ms.MetaClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;

public class MNSAgentUtil {
  private static final Log LOG = LogFactory.getLog(MNSAgentUtil.class);

  public static MNSClientAgent getMNSClientAgent(String accessKeyId,
      String accessKeySecret, String endpoint, boolean runLocal)
      throws Exception {
    Configuration conf = new Configuration();
    if (accessKeyId == null || accessKeySecret == null) {
      String stsAccessKeyId = MetaClient.getRoleAccessKeyId();
      String stsAccessKeySecret = MetaClient.getRoleAccessKeySecret();
      String securityToken = MetaClient.getRoleSecurityToken();
      return getMNSClientAgent(stsAccessKeyId, stsAccessKeySecret, securityToken,
          endpoint, conf, runLocal);
    } else {
      return getMNSClientAgent(accessKeyId, accessKeySecret, endpoint, conf,
          runLocal);
    }
  }

  @SuppressWarnings("unchecked")
  public static MNSClientAgent getMNSClientAgent(String accessKeyId,
      String accessKeySecret, String endpoint, Configuration conf,
      boolean runLocal) throws Exception {
    URLClassLoader classLoader = ResourceLoader.getInstance()
        .getUrlClassLoader(conf, runLocal);
    Class cloudAccountClz = classLoader
        .loadClass("com.aliyun.mns.client.CloudAccount");
    Class mnsClientClz = classLoader
        .loadClass("com.aliyun.mns.client.MNSClient");
    Constructor cons = cloudAccountClz
        .getConstructor(String.class, String.class, String.class);
    Object cloudAccount =
        cons.newInstance(accessKeyId, accessKeySecret, endpoint);
    Method method = cloudAccountClz.getMethod("getMNSClient");
    Object mnsClient = method.invoke(cloudAccount);
    return new MNSClientAgent(mnsClient, mnsClientClz, endpoint, classLoader);
  }

  @SuppressWarnings("unchecked")
  public static MNSClientAgent getMNSClientAgent(String accessKeyId,
       String accessKeySecret, String securityToken, String endpoint,
       Configuration conf, boolean runLocal) throws Exception {
    URLClassLoader classLoader = ResourceLoader.getInstance()
        .getUrlClassLoader(conf, runLocal);
    Class cloudAccountClz = classLoader
        .loadClass("com.aliyun.mns.client.CloudAccount");
    Class mnsClientClz = classLoader
        .loadClass("com.aliyun.mns.client.MNSClient");
    Constructor cons = cloudAccountClz
        .getConstructor(String.class, String.class, String.class, String.class);
    Object cloudAccount =
        cons.newInstance(accessKeyId, accessKeySecret, endpoint, securityToken);
    Method method = cloudAccountClz.getMethod("getMNSClient");
    Object mnsClient = method.invoke(cloudAccount);
    return new MNSClientAgent(mnsClient, mnsClientClz, endpoint, classLoader);
  }

  @SuppressWarnings("unchecked")
  public static Object updateMNSClient(Exception e, URLClassLoader classLoader,
      String endpoint) throws Exception{
    if (e instanceof InvocationTargetException) {
      Throwable t = ((InvocationTargetException) e).getTargetException();
      if (t.getMessage()
          .contains("The AccessKey Id you provided is not exist.")) {
        LOG.warn(t.getMessage() + " Try to get AK information from MetaService");
        String accessKeyId = MetaClient.getRoleAccessKeyId();
        String accessKeySecret = MetaClient.getRoleAccessKeySecret();
        String securityToken = MetaClient.getRoleSecurityToken();
        try {
          Class cloudAccountClz = classLoader
              .loadClass("com.aliyun.mns.client.CloudAccount");
          Class mnsClientClz = classLoader
              .loadClass("com.aliyun.mns.client.MNSClient");
          Constructor cons = cloudAccountClz
              .getConstructor(String.class, String.class, String.class,
                  String.class);
          Object cloudAccount =
              cons.newInstance(accessKeyId, accessKeySecret, endpoint,
                  securityToken);
          Method method = cloudAccountClz.getMethod("getMNSClient");
          Object mnsClient = method.invoke(cloudAccount);
          MNSClientAgent mnsClientAgent = new MNSClientAgent(mnsClient,
              mnsClientClz, endpoint, classLoader);
          try {
            // Test ths MNSClient works or not.
            mnsClientAgent.isOpen();
            return mnsClient;
          } catch (Exception e1) {
            LOG.warn(e1);
            return null;
          }
        } catch (Exception e2) {
          LOG.warn(e2);
          return null;
        }
      }
    }

    return null;
  }
}
