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

package com.aliyun.fs.oss.nat;

import com.aliyun.fs.oss.common.FileMetadata;
import com.aliyun.fs.oss.common.NativeFileSystemStore;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.fs.oss.common.OssException;
import com.aliyun.fs.oss.common.PartialListing;
import com.aliyun.fs.oss.utils.OSSCopyTask;
import com.aliyun.fs.oss.utils.Result;
import com.aliyun.fs.oss.utils.Task;
import com.aliyun.fs.oss.utils.TaskEngine;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.*;
import org.apache.hadoop.conf.Configuration;

public class JetOssNativeFileSystemStore implements NativeFileSystemStore{

    private Long MAX_COPY_SIZE = 128 * 1024 * 1024L;

    private OSSClient ossClient;
    private String bucket;
    private Boolean enableMultiPart;
    private int numCopyThreads;

    public void initialize(URI uri, Configuration conf) throws IOException {
        String endpoint = conf.get("fs.oss.endpoint");
        String accessKeyId = conf.get("fs.oss.accessKeyId");
        String accessKeySecret = conf.get("fs.oss.accessKeySecret");
        String securityToken = conf.get("fs.oss.securityToken");
        if (securityToken == null) {
            this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret);
        } else {
            this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret, securityToken);
        }
        this.bucket = uri.getHost();
        this.enableMultiPart = conf.getBoolean("fs.oss.multipart.enable", true);
        this.numCopyThreads = conf.getInt("fs.oss.multipart.thread.number", 5);
    }

    public void storeFile(String key, File file, boolean append)
            throws IOException {

        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            ObjectMetadata objMeta = new ObjectMetadata();
            if (!append) {
                objMeta.setContentLength(file.length());
                ossClient.putObject(bucket, key, in, objMeta);
            } else {
                if (!checkAppendableFile(key)) {
                    AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucket, key, file);
                    appendObjectRequest.setPosition(0L);
                    ossClient.appendObject(appendObjectRequest);
                } else {
                    ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, key);
                    Long preContentLength = objectMetadata.getContentLength();
                    AppendObjectRequest appendObjectRequest = new AppendObjectRequest(bucket, key, file);
                    appendObjectRequest.setPosition(preContentLength);
                    ossClient.appendObject(appendObjectRequest);
                }
            }
        } catch (ServiceException e) {
            handleServiceException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public void storeEmptyFile(String key) throws IOException {
        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            objMeta.setContentLength(0);
            ossClient.putObject(bucket, key, new ByteArrayInputStream(new byte[0]), objMeta);
        } catch (ServiceException e) {
            handleServiceException(e);
        }
    }

    public FileMetadata retrieveMetadata(String key) throws IOException {
        try {
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, key);
            return new FileMetadata(key, objectMetadata.getContentLength(),
                    objectMetadata.getLastModified().getTime());
        } catch (ServiceException e) {
            // Following is brittle. Is there a better way?
            if (e.getMessage().contains("ResponseCode=404")) {
                return null;
            }
            return null; //never returned - keep compiler happy
        }
    }

    public InputStream retrieve(String key) throws IOException {
        try {
            OSSObject object = ossClient.getObject(bucket, key);
            return object.getObjectContent();
        } catch (ServiceException e) {
            handleServiceException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    public InputStream retrieve(String key, long byteRangeStart)
            throws IOException {
        try {
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, key);
            long fileSize = objectMetadata.getContentLength();
            GetObjectRequest getObjReq = new GetObjectRequest(bucket, key);
            getObjReq.setRange(byteRangeStart, fileSize - 1);
            OSSObject object = ossClient.getObject(getObjReq);
            return object.getObjectContent();
        } catch (ServiceException e) {
            handleServiceException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {
        return list(prefix, maxListingLength, null, false);
    }

    public PartialListing list(String prefix, int maxListingLength, String priorLastKey,
                               boolean recurse) throws IOException {

        return list(prefix, recurse ? null : NativeOssFileSystem.PATH_DELIMITER, maxListingLength, priorLastKey);
    }


    private PartialListing list(String prefix, String delimiter,
                                int maxListingLength, String priorLastKey) throws IOException {
        try {
            if (prefix.length() > 0 && !prefix.endsWith(NativeOssFileSystem.PATH_DELIMITER)) {
                prefix += NativeOssFileSystem.PATH_DELIMITER;
            }
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);
            listObjectsRequest.setDelimiter(delimiter);
            listObjectsRequest.setMarker(priorLastKey);
            listObjectsRequest.setMaxKeys(maxListingLength);
            listObjectsRequest.setPrefix(prefix);

            ObjectListing listing = ossClient.listObjects(listObjectsRequest);
            List<OSSObjectSummary> objects = listing.getObjectSummaries();

            FileMetadata[] fileMetadata =
                    new FileMetadata[objects.size()];
            Iterator<OSSObjectSummary> iter = objects.iterator();
            int idx = 0;
            while(iter.hasNext()) {
                OSSObjectSummary obj = iter.next();
                fileMetadata[idx] = new FileMetadata(obj.getKey(),
                        obj.getSize(), obj.getLastModified().getTime());
                idx += 1;
            }
            return new PartialListing(listing.getNextMarker(), fileMetadata, listing.getCommonPrefixes().toArray(new String[0]));
        } catch (ServiceException e) {
            handleServiceException(e);
            return null; //never returned - keep compiler happy
        }
    }

    public void delete(String key) throws IOException {
        try {
            ossClient.deleteObject(bucket, key);
        } catch (ServiceException e) {
            handleServiceException(key, e);
        }
    }

    public void copy(String srcKey, String dstKey) throws IOException {
        try {
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, srcKey);
            Long contentLength = objectMetadata.getContentLength();
            if (contentLength <= MAX_COPY_SIZE) {
                ossClient.copyObject(bucket, srcKey, bucket, dstKey);
            } else {
                InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                        new InitiateMultipartUploadRequest(bucket, dstKey);
                InitiateMultipartUploadResult initiateMultipartUploadResult =
                        ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
                String uploadId = initiateMultipartUploadResult.getUploadId();
                Long partSize = contentLength / numCopyThreads;
                int partCount = numCopyThreads;
                if (contentLength % partSize != 0) {
                    partCount++;
                }
                List<PartETag> partETags = new ArrayList<PartETag>();
                if (enableMultiPart) {
                    List<Task> tasks = new ArrayList<Task>();
                    for (int i = 0; i < partCount; i++) {
                        long skipBytes = partSize * i;
                        long size = partSize < contentLength - skipBytes ? partSize : contentLength - skipBytes;
                        OSSCopyTask ossCopyTask = new OSSCopyTask(
                                ossClient, uploadId, bucket, bucket, srcKey, dstKey, size, skipBytes, i+1);
                        ossCopyTask.setUuid(i+"");
                        tasks.add(ossCopyTask);
                    }
                    TaskEngine taskEngine = new TaskEngine(tasks, numCopyThreads, numCopyThreads);
                    try {
                        taskEngine.executeTask();
                        Map<String, Object> responseMap = taskEngine.getResultMap();
                        for (int i = 0; i < partCount; i++) {
                            UploadPartCopyResult uploadPartCopyResult = (UploadPartCopyResult)
                                    ((Result) responseMap.get(i+"")).getModels().get("uploadPartCopyResult");
                            partETags.add(uploadPartCopyResult.getPartETag());
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        taskEngine.shutdown();
                    }
                } else {
                    for (int i = 0; i < partCount; i++) {
                        long skipBytes = partSize * i;
                        long size = partSize < contentLength - skipBytes ? partSize : contentLength - skipBytes;
                        UploadPartCopyRequest uploadPartCopyRequest = new UploadPartCopyRequest(bucket, srcKey, bucket, dstKey);
                        uploadPartCopyRequest.setUploadId(uploadId);
                        uploadPartCopyRequest.setPartSize(size);
                        uploadPartCopyRequest.setBeginIndex(skipBytes);
                        uploadPartCopyRequest.setPartNumber(i + 1);
                        UploadPartCopyResult uploadPartCopyResult = ossClient.uploadPartCopy(uploadPartCopyRequest);
                        partETags.add(uploadPartCopyResult.getPartETag());
                    }
                }

                CompleteMultipartUploadRequest completeMultipartUploadRequest =
                        new CompleteMultipartUploadRequest(bucket, dstKey, uploadId, partETags);
                ossClient.completeMultipartUpload(completeMultipartUploadRequest);
            }
        } catch (ServiceException e) {
            handleServiceException(srcKey, e);
        }
    }

    public void purge(String prefix) throws IOException {
        try {
            List<OSSObjectSummary> objects = ossClient.listObjects(bucket, prefix).getObjectSummaries();
            for(OSSObjectSummary ossObjectSummary: objects) {
                ossClient.deleteObject(bucket, ossObjectSummary.getKey());
            }
        } catch (ServiceException e) {
            handleServiceException(e);
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("OSS Native Filesystem, ");
        sb.append(bucket).append("\n");
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);
            List<OSSObjectSummary> objects = ossClient.listObjects(listObjectsRequest).getObjectSummaries();
            for(OSSObjectSummary ossObjectSummary: objects) {
                sb.append(ossObjectSummary.getKey()).append("\n");
            }
        } catch (ServiceException e) {
            handleServiceException(e);
        }
        System.out.println(sb);
    }

    private void handleServiceException(String key, ServiceException e) throws IOException {
        if ("NoSuchKey".equals(e.getErrorCode())) {
            throw new FileNotFoundException("Key '" + key + "' does not exist in OSS");
        } else {
            handleServiceException(e);
        }
    }

    private void handleServiceException(ServiceException e) throws IOException {
        if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        }
        else {
            throw new OssException(e);
        }
    }

    private boolean checkAppendableFile(String key) {
        try {
            ossClient.getObject(bucket, key);
            return true;
        } catch (OSSException e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return false;
            } else {
                throw new OssException(e);
            }
        }
    }
}
