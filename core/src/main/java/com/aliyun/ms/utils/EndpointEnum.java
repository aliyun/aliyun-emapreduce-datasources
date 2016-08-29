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
package com.aliyun.ms.utils;

public enum EndpointEnum {

    OSS_CN_HANGZHOU_CLASSICAL("oss", "cn-hangzhou", "classical", "oss-cn-hangzhou-internal.aliyuncs.com"),
    OSS_CN_SHANGHAI_CLASSICAL("oss", "cn-shanghai", "classical", "oss-cn-shanghai-internal.aliyuncs.com"),
    OSS_CN_QINGDAO_CLASSICAL("oss", "cn-qingdao", "classical", "oss-cn-qingdao-internal.aliyuncs.com"),
    OSS_CN_BEIJING_CLASSICAL("oss", "cn-beijing", "classical", "oss-cn-beijing-internal.aliyuncs.com"),
    OSS_CN_SHENZHEN_CLASSICAL("oss", "cn-shenzhen", "classical", "oss-cn-shenzhen-internal.aliyuncs.com"),
    OSS_CN_HONGKONG_CLASSICAL("oss", "cn-hongkong", "classical", "oss-cn-hongkong-internal.aliyuncs.com"),
    OSS_US_WEST_1_CLASSICAL("oss", "us-west-1", "classical", "oss-us-west-1-internal.aliyuncs.com"),
    OSS_US_EAST_1_CLASSICAL("oss", "us-east-1", "classical", "oss-us-east-1-internal.aliyuncs.com"),
    OSS_AP_SOUTHEAST_1_CLASSICAL("oss", "ap-southeast-1", "classical", "oss-ap-southeast-1-internal.aliyuncs.com"),

    OSS_CN_HANGZHOU_VPC("oss", "cn-hangzhou", "vpc", "vpc100-oss-cn-hangzhou.aliyuncs.com"),
    OSS_CN_SHANGHAI_VPC("oss", "cn-shanghai", "vpc", "vpc100-oss-cn-shanghai.aliyuncs.com"),
    OSS_CN_QINGDAO_VPC("oss", "cn-qingdao", "vpc", "vpc100-oss-cn-qingdao.aliyuncs.com"),
    OSS_CN_BEIJING_VPC("oss", "cn-beijing", "vpc", "vpc100-oss-cn-beijing.aliyuncs.com"),
    OSS_CN_SHENZHEN_VPC("oss", "cn-shenzhen", "vpc", "vpc100-oss-cn-shenzhen.aliyuncs.com"),
    OSS_US_WEST_1_VPC("oss", "us-west-1", "vpc", "vpc100-oss-us-west-1.aliyuncs.com"),
    OSS_US_EAST_1_VPC("oss", "us-east-1", "vpc", "oss-us-east-1-internal.aliyuncs.com"),
    OSS_AP_SOUTHEAST_1_VPC("oss", "ap-southeast-1", "vpc", "vpc100-oss-ap-southeast-1.aliyuncs.com"),

    ILLEGAL_ENDPOINT("illegal", "illegal", "illegal", "illegal");

    private String productCode;
    private String region;
    private String nType;
    private String endpoint;

    EndpointEnum(String productCode, String region, String nType, String endpoint) {
        this.productCode = productCode;
        this.region = region;
        this.nType = nType;
        this.endpoint = endpoint;
    }

    public static String getEndpoint(String productCode, String region, String nType) {
        for(EndpointEnum endpoint: EndpointEnum.values()) {
            if (endpoint.productCode.equals(productCode.toLowerCase()) &&
                    endpoint.region.equals(region.toLowerCase()) &&
                    endpoint.nType.equals(nType.toLowerCase())) {
                return endpoint.endpoint;
            }
        }
        return null;
    }
}
