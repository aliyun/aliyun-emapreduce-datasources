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
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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

  static final String metaServiceUrl = "http://100.100.100.200/latest/meta-data/";

  static final OkHttpClient clientV2 = new OkHttpClient();

  static private String trySend(String host, String url) {
    String finalUrl = "http://" + host + ":" + port + url;
    try {
      return HttpClientUtil.get(finalUrl);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  static private String trySendV2(String metaItem) {
    String finalUrl = metaServiceUrl + metaItem;
    try {
      Request request = new Request.Builder()
              .url(finalUrl)
              .build();
      Response response = clientV2.newCall(request).execute();
      if (response.code() != 200 || response.body() == null) {
        LOG.debug("failed to get response from clientV2");
        return null;
      }
      return response.body().string();
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  static public String getEMRCredentials(String type) {
    String EMRRole = trySendV2("Ram/security-credentials/");
    if (EMRRole == null)
      return null;
    String credentials = trySendV2("Ram/security-credentials/" + EMRRole);
    if (credentials == null)
      return null;
    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(credentials).getAsJsonObject();
    if (jsonObject.get("Code").getAsString().equals("Success")) {
      return jsonObject.get(type).getAsString();
    } else {
      LOG.debug("use old way to get ststoken");
      return null;
    }
  }
  static public String getClusterRegionName() {
    String regionName = trySendV2("region-id");
    if (regionName == null) {
      regionName = trySend("localhost", CLUSTER_REGION_URL);
    }
    return regionName;
  }

  static public String getClusterNetworkType() {
    String networkType = trySendV2("network-type");
    if (networkType == null) {
      networkType = trySend("localhost", CLUSTER_NETWORK_TYPE_URL);
    }
    return networkType;
  }

  static public String getClusterRoleName() {
    return trySend("localhost", CLUSTER_ROLE_NAME_URL);
  }

  static public String getRoleAccessKeyId() {
    String AccessKeyId = getEMRCredentials("AccessKeyId");
    if (AccessKeyId == null) {
      AccessKeyId = trySend("localhost", ROLE_ACCESS_KEY_ID_URL);
    }
    return AccessKeyId;
  }

  static public String getRoleAccessKeySecret() {
    String AccessKeySecret = getEMRCredentials("AccessKeySecret");
    if (AccessKeySecret == null) {
      AccessKeySecret = trySend("localhost", ROLE_ACCESS_KEY_SECRET_URL);
    }
    return AccessKeySecret;
  }

  static public String getRoleSecurityToken() {
    String SecurityToken = getEMRCredentials("SecurityToken");
    if (SecurityToken == null) {
      SecurityToken = trySend("localhost", ROLE_SECURITY_TOKEN_URL);
    }
    return SecurityToken;
  }

}
