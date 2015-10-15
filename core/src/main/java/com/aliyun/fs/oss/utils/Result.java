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
