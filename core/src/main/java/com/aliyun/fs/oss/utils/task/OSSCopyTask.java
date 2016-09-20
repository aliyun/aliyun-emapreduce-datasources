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
import com.aliyun.oss.model.UploadPartCopyResult;
import org.apache.hadoop.conf.Configuration;

public class OSSCopyTask extends Task {
    OSSClientAgent ossClient;
    private String uploadId;
    private String srcBucket;
    private String dstBucket;
    private String srcKey;
    private String dstKey;
    private Long partSize;
    private Long beginIndex;
    private int partNumber;
    private Configuration conf;

    public OSSCopyTask(OSSClientAgent ossClient,
                       String uploadId,
                       String srcBucket,
                       String dstBucket,
                       String srcKey,
                       String dstKey,
                       Long partSize,
                       Long beginIndex,
                       int partNumber,
                       Configuration conf) {
        this.ossClient = ossClient;
        this.uploadId = uploadId;
        this.srcBucket = srcBucket;
        this.srcKey  =srcKey;
        this.dstBucket = dstBucket;
        this.dstKey = dstKey;
        this.partSize = partSize;
        this.partNumber = partNumber;
        this.beginIndex = beginIndex;
        this.conf = conf;
    }

    @Override
    public void execute(TaskEngine engineRef) {
        Result result = new Result();
        try {
            UploadPartCopyResult uploadPartCopyResult = ossClient.uploadPartCopy(uploadId, srcBucket, dstBucket, srcKey,
                    dstKey, partSize, beginIndex, partNumber, conf);
            result.getModels().put("uploadPartCopyResult", uploadPartCopyResult);
            // TODO: fail?
            result.setSuccess(true);
            this.response = result;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
