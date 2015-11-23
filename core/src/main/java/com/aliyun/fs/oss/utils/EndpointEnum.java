/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.fs.oss.utils;

import java.util.ArrayList;
import java.util.List;

public enum EndpointEnum {
    CN_HANGZHOU_INTERNET("cn-hangzhou", "oss-cn-hangzhou.aliyuncs.com"),
    CN_HANGZHOU_INTRANET("cn-hangzhou-internal", "oss-cn-hangzhou-internal.aliyuncs.com"),
    CN_BEIJING_INTERNET("cn-beijing", "oss-cn-beijing.aliyuncs.com"),
    CN_BEIJING_INTRANET("cn-beijing-internal", "oss-cn-beijing-internal.aliyuncs.com"),
    CN_QINGDAO_INTERNAT("cn-qingdao", "oss-cn-qingdao.aliyuncs.com"),
    CN_QINGDAO_INTRANET("cn-qingdao-internal", "oss-cn-qingdao-internal.aliyuncs.com"),
    CN_HONGKONG_INTERNET("cn-hongkong", "oss-cn-hongkong.aliyuncs.com"),
    CN_HONGKONG_INTRANET("cn-hongkong-internal", "oss-cn-hongkong-internal.aliyuncs.com"),
    CN_SHENZHEN_INTERNET("cn-shenzhen", "oss-cn-shenzhen.aliyuncs.com"),
    CN_SHENZHEN_INTRANET("cn-shenzhen-internal", "oss-cn-shenzhen-internal.aliyuncs.com"),
    CN_SHANGHAI_INTERNET("cn-shanghai", "oss-cn-shanghai.aliyuncs.com"),
    CN_SHANGHAI_INTRANET("cn-shanghai-internal", "oss-cn-shanghai-internal.aliyuncs.com"),
    US_WEST_1_INTERNET("us-west-1", "oss-us-west-1.aliyuncs.com"),
    US_WEST_1_INTRANET("us-west-1-internal", "oss-us-west-1-internal.aliyuncs.com"),
    AP_SOUTHEAST_1_INTERNET("ap-southeast-1", "oss-ap-southeast-1.aliyuncs.com"),
    AP_SOUTHEAST_1_INTRANET("ap-southeast-1-internal", "oss-ap-southeast-1-internal.aliyuncs.com");

    private String name;
    private String location;

    private EndpointEnum(String name, String location) {
        this.name = name;
        this.location = location;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public static List<EndpointEnum> getInternalEndpoints() {
        List<EndpointEnum> internalEndpoints = new ArrayList<EndpointEnum>();
        for(EndpointEnum enpoint: EndpointEnum.values()) {
            if (enpoint.getName().endsWith("internal")) {
                internalEndpoints.add(enpoint);
            }
        }

        return internalEndpoints;
    }
}
