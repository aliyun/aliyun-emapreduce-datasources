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
package com.aliyun.fs.oss.utils.task;

import com.aliyun.fs.oss.utils.OSSClientAgent;
import com.aliyun.fs.oss.utils.Result;
import com.aliyun.fs.oss.utils.Task;
import com.aliyun.fs.oss.utils.TaskEngine;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

import java.io.*;

public class OSSPutTask extends Task {
    OSSClientAgent ossClient;
    private String uploadId;
    private String bucket;
    private String key;
    private Long partSize;
    private Long beginIndex;
    private int partNumber;
    private File localFile;

    public OSSPutTask(OSSClientAgent ossClient,
                       String uploadId,
                       String bucket,
                       String key,
                       Long partSize,
                       Long beginIndex,
                       int partNumber,
                       File file) {
        this.ossClient = ossClient;
        this.uploadId = uploadId;
        this.bucket = bucket;
        this.key = key;
        this.partSize = partSize;
        this.beginIndex = beginIndex;
        this.partNumber = partNumber;
        this.localFile = file;
    }

    @Override
    public void execute(TaskEngine engineRef) {
        Result result = new Result();
        try {
            UploadPartResult uploadPartResult = ossClient.uploadPart(uploadId, bucket, key, partSize, beginIndex,
                    partNumber, localFile);
            result.getModels().put("uploadPartResult", uploadPartResult);
            // TODO: fail?
            result.setSuccess(true);
            this.response = result;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
