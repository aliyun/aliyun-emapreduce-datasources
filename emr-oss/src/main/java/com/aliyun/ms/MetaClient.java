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
  static final String PORT = "10011";
  static final String CLUSTER_REGION_URL = "/cluster-region";
  static final String CLUSTER_NETWORK_TYPE_URL = "/cluster-network-type";
  static final String CLUSTER_ROLE_NAME_URL = "/cluster-role-name";
  static final String ROLE_ACCESS_KEY_ID_URL = "/role-access-key-id";
  static final String ROLE_ACCESS_KEY_SECRET_URL = "/role-access-key-secret";
  static final String ROLE_SECURITY_TOKEN_URL = "/role-security-token";

  static final String META_SERVICE_URL = "http://100.100.100.200/latest/meta-data/";

  static final OkHttpClient ECS_META_CLIENT = new OkHttpClient();

  private static String requestMetaFromEmr(String host, String url) {
    String finalUrl = "http://" + host + ":" + PORT + url;
    try {
      return HttpClientUtil.get(finalUrl);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  private static String requestMetaFromEcs(String metaItem) {
    String finalUrl = META_SERVICE_URL + metaItem;
    try {
      Request request = new Request.Builder()
              .url(finalUrl)
              .build();
      Response response = ECS_META_CLIENT.newCall(request).execute();
      if (response.code() != 200 || response.body() == null) {
        LOG.error("failed to get response from ecsMeta, http code: " + response.code());
        return null;
      }
      return response.body().string();
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  /**
   * use old way to get ststoken if return null
   **/
  private static String getEmrCredentials(String type) {
    String emrRole = requestMetaFromEcs("Ram/security-credentials/");
    if (emrRole == null) {
      return null;
    }
    String credentials = requestMetaFromEcs("Ram/security-credentials/" + emrRole);
    if (credentials == null) {
      return null;
    }
    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(credentials).getAsJsonObject();
    if (jsonObject.get("Code").getAsString().equals("Success")) {
      return jsonObject.get(type).getAsString();
    } else {
      LOG.warn("use old way to get ststoken");
      return null;
    }
  }
  public static String getClusterRegionName() {
    String regionName = requestMetaFromEcs("region-id");
    if (regionName == null) {
      regionName = requestMetaFromEmr("localhost", CLUSTER_REGION_URL);
    }
    return regionName;
  }

  public static String getClusterNetworkType() {
    String networkType = requestMetaFromEcs("network-type");
    if (networkType == null) {
      networkType = requestMetaFromEmr("localhost", CLUSTER_NETWORK_TYPE_URL);
    }
    return networkType;
  }

  public static String getClusterRoleName() {
    return requestMetaFromEmr("localhost", CLUSTER_ROLE_NAME_URL);
  }

  public static String getRoleAccessKeyId() {
    String accessKeyId = getEmrCredentials("accessKeyId");
    if (accessKeyId == null) {
      accessKeyId = requestMetaFromEmr("localhost", ROLE_ACCESS_KEY_ID_URL);
    }
    return accessKeyId;
  }

  public static String getRoleAccessKeySecret() {
    String accessKeySecret = getEmrCredentials("accessKeySecret");
    if (accessKeySecret == null) {
      accessKeySecret = requestMetaFromEmr("localhost", ROLE_ACCESS_KEY_SECRET_URL);
    }
    return accessKeySecret;
  }

  public static String getRoleSecurityToken() {
    String securityToken = getEmrCredentials("securityToken");
    if (securityToken == null) {
      securityToken = requestMetaFromEmr("localhost", ROLE_SECURITY_TOKEN_URL);
    }
    return securityToken;
  }
}
