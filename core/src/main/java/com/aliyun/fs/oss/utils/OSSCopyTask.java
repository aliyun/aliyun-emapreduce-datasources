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

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;

import java.util.UUID;

public class OSSCopyTask extends Task {
    OSSClient ossClient;
    private String uploadId;
    private String srcBucket;
    private String dstBucket;
    private String srcKey;
    private String dstKey;
    private Long partSize;
    private Long beginIndex;
    private int partNumber;

    public OSSCopyTask(OSSClient ossClient,
                       String uploadId,
                       String srcBucket,
                       String dstBucket,
                       String srcKey,
                       String dstKey,
                       Long partSize,
                       Long beginIndex,
                       int partNumber) {
        this.ossClient = ossClient;
        this.uploadId = uploadId;
        this.srcBucket = srcBucket;
        this.srcKey  =srcKey;
        this.dstBucket = dstBucket;
        this.dstKey = dstKey;
        this.partSize = partSize;
        this.partNumber = partNumber;
        this.beginIndex = beginIndex;
    }

    @Override
    public void execute(TaskEngine engineRef) {
        Result result = new Result();
        UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest(srcBucket, srcKey, dstBucket, dstKey);
        uploadPartCopyRequest.setUploadId(uploadId);
        uploadPartCopyRequest.setPartSize(partSize);
        uploadPartCopyRequest.setBeginIndex(beginIndex);
        uploadPartCopyRequest.setPartNumber(partNumber);
        UploadPartCopyResult uploadPartCopyResult = ossClient.uploadPartCopy(uploadPartCopyRequest);
        result.getModels().put("uploadPartCopyResult", uploadPartCopyResult);
        // TODO: fail?
        result.setSuccess(true);

        this.response = result;
    }
}
