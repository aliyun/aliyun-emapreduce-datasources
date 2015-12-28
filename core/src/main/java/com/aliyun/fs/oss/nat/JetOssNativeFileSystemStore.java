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
import com.aliyun.fs.oss.utils.*;
import com.aliyun.fs.oss.utils.task.OSSCopyTask;
import com.aliyun.fs.oss.utils.task.OSSPutTask;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class JetOssNativeFileSystemStore implements NativeFileSystemStore{
    private static final Log LOG = LogFactory.getLog(JetOssNativeFileSystemStore.class);
    private int numSplitsUpperLimit = 10000;

    private Long maxSimpleCopySize;
    private Long maxSimplePutSize;

    private OSSClient ossClient;
    private String bucket;
    private int numCopyThreads;
    private int maxSplitSize;
    private int numSplits;

    private String endpoint = null;
    private String accessKeyId = null;
    private String accessKeySecret = null;
    private String securityToken = null;

    public void initialize(URI uri, Configuration conf) throws IOException {
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("Invalid hostname in URI " + uri);
        }
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] ossCredentials  = userInfo.split(":");
            if (ossCredentials.length >= 2) {
                accessKeyId = ossCredentials[0];
                accessKeySecret = ossCredentials[1];
            }
            if (ossCredentials.length == 3) {
                securityToken = ossCredentials[2];
            }
        }

        String host = uri.getHost();
        if (!StringUtils.isEmpty(host) && !host.contains(".")) {
            bucket = host;
        } else if (!StringUtils.isEmpty(host)) {
            bucket = host.substring(0, host.indexOf("."));
            endpoint = host.substring(host.indexOf(".") + 1);
        }

        if (accessKeyId == null) {
            accessKeyId = conf.getTrimmed("fs.oss.accessKeyId");
        }
        if (accessKeySecret == null) {
            accessKeySecret = conf.getTrimmed("fs.oss.accessKeySecret");
        }
        if (securityToken == null) {
            securityToken = conf.getTrimmed("fs.oss.securityToken");
        }

        if (endpoint == null) {
            endpoint = conf.getTrimmed("fs.oss.endpoint");
        }

        ClientConfiguration cc = initializeOSSClientConfig(conf);

        if (securityToken == null) {
            this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret, cc);
        } else {
            this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret, securityToken, cc);
        }
        this.numCopyThreads = conf.getInt("fs.oss.multipart.thread.number", 5);
        this.maxSplitSize = conf.getInt("fs.oss.multipart.split.max.byte", 5 * 1024 * 1024);
        this.numSplits = conf.getInt("fs.oss.multipart.split.number", numCopyThreads);
        this.maxSimpleCopySize = conf.getLong("fs.oss.copy.simple.max.byte", 64 * 1024 * 1024L);
        this.maxSimplePutSize = conf.getLong("fs.oss.put.simple.max.byte", 64 * 1024 * 1024);
    }

    public void storeFile(String key, File file, boolean append)
            throws IOException {
        BufferedInputStream in = null;

        try {
            in = new BufferedInputStream(new FileInputStream(file));
            ObjectMetadata objMeta = new ObjectMetadata();
            if (!append) {
                Long fileLength = file.length();
                if (fileLength < Math.min(maxSimplePutSize, 512 * 1024 * 1024L)) {
                    objMeta.setContentLength(file.length());
                    ossClient.putObject(bucket, key, in, objMeta);
                } else {
                    LOG.info("using multipart upload for key " + key + ", size: " + fileLength);
                    doMultipartPut(file, key);
                }
            } else {
                if (!doesObjectExist(key)) {
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
            if (!doesObjectExist(key)) {
                return null;
            }
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
            if (!doesObjectExist(key)) {
                return null;
            }
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
            if (!doesObjectExist(key)) {
                return null;
            }
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
            if (!doesObjectExist(srcKey)) {
                return;
            }
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, srcKey);
            Long contentLength = objectMetadata.getContentLength();
            if (contentLength <= Math.min(maxSimpleCopySize, 512 * 1024 * 1024L)) {
                ossClient.copyObject(bucket, srcKey, bucket, dstKey);
            } else {
                LOG.info("using multipart copy for copying from srckey " + srcKey + " to " + dstKey +
                        ", size: " + contentLength);
                doMultipartCopy(srcKey, dstKey, contentLength);
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

    private boolean doesObjectExist(String key) {
        try {
            return ossClient.doesObjectExist(bucket, key);
        } catch (Exception e) {
            return false;
        }
    }

    private void doMultipartCopy(String srcKey, String dstKey, Long contentLength) {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucket, dstKey);
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        String uploadId = initiateMultipartUploadResult.getUploadId();
        Long minSplitSize = contentLength / numSplitsUpperLimit + 1;
        Long partSize = Math.max(Math.min(maxSplitSize, contentLength / numSplits), minSplitSize);
        int partCount = (int) (contentLength / partSize);
        if (contentLength % partSize != 0) {
            partCount++;
        }
        LOG.info("multipart copying, partCount" + partCount + ", partSize " + partSize);
        List<PartETag> partETags = new ArrayList<PartETag>();
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

        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucket, dstKey, uploadId, partETags);
        ossClient.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private void doMultipartPut(File file, String key) throws IOException {
        Long contentLength = file.length();
        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucket, key);
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClient.initiateMultipartUpload(initiateMultipartUploadRequest);
        String uploadId = initiateMultipartUploadResult.getUploadId();
        Long minSplitSize = contentLength / numSplitsUpperLimit + 1;
        Long partSize = Math.max(Math.min(maxSplitSize, contentLength / numSplits), minSplitSize);
        int partCount = (int) (contentLength / partSize);
        if (contentLength % partSize != 0) {
            partCount++;
        }
        LOG.info("multipart uploading, partCount" + partCount + ", partSize " + partSize);
        List<PartETag> partETags = new ArrayList<PartETag>();
        List<Task> tasks = new ArrayList<Task>();
        for (int i = 0; i < partCount; i++) {
            long skipBytes = partSize * i;
            long size = partSize < contentLength - skipBytes ? partSize : contentLength - skipBytes;
            OSSPutTask ossPutTask = new OSSPutTask(ossClient, uploadId, bucket, key, size, skipBytes, i+1, file);
            ossPutTask.setUuid(i + "");
            tasks.add(ossPutTask);
        }
        TaskEngine taskEngine = new TaskEngine(tasks, numCopyThreads, numCopyThreads);
        try {
            taskEngine.executeTask();
            Map<String, Object> responseMap = taskEngine.getResultMap();
            for (int i = 0; i < partCount; i++) {
                UploadPartResult uploadPartResult = (UploadPartResult)
                        ((Result) responseMap.get(i+"")).getModels().get("uploadPartResult");
                partETags.add(uploadPartResult.getPartETag());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            taskEngine.shutdown();
        }

        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags);
        ossClient.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private ClientConfiguration initializeOSSClientConfig(Configuration conf) {
        ClientConfiguration cc = new ClientConfiguration();
        cc.setConnectionTimeout(conf.getInt("fs.oss.client.connection.timeout",
                ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT));
        cc.setSocketTimeout(conf.getInt("fs.oss.client.socket.timeout",
                ClientConfiguration.DEFAULT_SOCKET_TIMEOUT));
        cc.setConnectionTTL(conf.getLong("fs.oss.client.connection.ttl",
                ClientConfiguration.DEFAULT_CONNECTION_TTL));
        cc.setMaxConnections(conf.getInt("fs.oss.connection.max",
                ClientConfiguration.DEFAULT_MAX_CONNECTIONS));

        return cc;
    }
}
