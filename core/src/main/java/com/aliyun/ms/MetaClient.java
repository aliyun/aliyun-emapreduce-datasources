/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.ms;

import com.aliyun.ms.utils.HttpClientUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class MetaClient {
  static final Log LOG = LogFactory.getLog(MetaClient.class);
  static final String port = "10011";
  static final String CLUSTER_REGION_URL = "/cluster-region";
  static final String CLUSTER_NETWORK_TYPE_URL = "/cluster-network-type";
  static final String CLUSTER_ROLE_NAME_URL = "/cluster-role-name";
  static final String ROLE_ACCESS_KEY_ID_URL = "/role-access-key-id";
  static final String ROLE_ACCESS_KEY_SECRET_URL = "/role-access-key-secret";
  static final String ROLE_SECURITY_TOKEN_URL = "/role-security-token";

  static private String trySend(String host, String url) {
    String finalUrl = "http://" + host + ":" + port + url;
    try {
      return HttpClientUtil.get(finalUrl);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  static public String getClusterRegionName() {
    return trySend("localhost", CLUSTER_REGION_URL);
  }

  static public String getClusterNetworkType() {
    return trySend("localhost", CLUSTER_NETWORK_TYPE_URL);
  }

  static public String getClusterRoleName() {
    return trySend("localhost", CLUSTER_ROLE_NAME_URL);
  }

  static public String getRoleAccessKeyId() {
    return trySend("localhost", ROLE_ACCESS_KEY_ID_URL);
  }

  static public String getRoleAccessKeySecret() {
    return trySend("localhost", ROLE_ACCESS_KEY_SECRET_URL);
  }

  static public String getRoleSecurityToken() {
    return trySend("localhost", ROLE_SECURITY_TOKEN_URL);
  }
}
