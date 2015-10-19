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

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.Map;

public class Result {
    public static Result FAIL = new Result(false, "FAIL");

    public Result() {
    }

    public Result(boolean isSuccess, String errorMsg) {
        this.isSuccess = isSuccess;
        this.errorMsg = errorMsg;
    }

    private boolean isSuccess = true;

    private String errorMsg;

    private String errorCode;

    private String desc;

    private Map<String,Object> models = new HashMap<String, Object>();

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void setErrorMsg(String ... msgList) {
        this.errorMsg = Joiner.on(" ").join(msgList);
    }

    public Map<String, Object> getModels() {
        return models;
    }

    public void setModels(Map<String, Object> models) {
        this.models = models;
    }

    public void addModel(String key, Object value) {
        models.put(key, value);
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public void setErrorCode(String errorCode, String errorMsg) {
        this.isSuccess = false;
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }
}
