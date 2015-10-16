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
