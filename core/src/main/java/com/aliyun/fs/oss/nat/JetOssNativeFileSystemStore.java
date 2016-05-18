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

    private Configuration conf;

    private OSSClientAgent ossClient;
    private String bucket;
    private int numCopyThreads;
    private int maxSplitSize;
    private int numSplits;

    private String endpoint = null;
    private String accessKeyId = null;
    private String accessKeySecret = null;
    private String securityToken = null;

    public void initialize(URI uri, Configuration conf) throws Exception {
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

        this.conf = conf;
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

        if (securityToken == null) {
            this.ossClient = new OSSClientAgent(endpoint, accessKeyId, accessKeySecret, conf);
        } else {
            this.ossClient = new OSSClientAgent(endpoint, accessKeyId, accessKeySecret, securityToken, conf);
        }
        this.numCopyThreads = conf.getInt("fs.oss.multipart.thread.number", 5);
        this.maxSplitSize = conf.getInt("fs.oss.multipart.split.max.byte", 5 * 1024 * 1024);
        this.numSplits = conf.getInt("fs.oss.multipart.split.number", numCopyThreads);
        this.maxSimpleCopySize = conf.getLong("fs.oss.copy.simple.max.byte", 64 * 1024 * 1024L);
        this.maxSimplePutSize = conf.getLong("fs.oss.put.simple.max.byte", 64 * 1024 * 1024);
    }

    public void storeFile(String key, File file, boolean append) throws IOException {
        try {
            if (!append) {
                Long fileLength = file.length();
                if (fileLength < Math.min(maxSimplePutSize, 512 * 1024 * 1024L)) {
                    ossClient.putObject(bucket, key, file);
                } else {
                    LOG.info("using multipart upload for key " + key + ", size: " + fileLength);
                    doMultipartPut(file, key);
                }
            } else {
                throw new IOException("'append' not supported.");
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    @Override
    public void storeFiles(String key, List<File> files, boolean append) throws IOException {
        try {
            if (files.size() == 1 && files.get(0).length() < Math.min(maxSimplePutSize, 512 * 1024 * 1024L)) {
                ossClient.putObject(bucket, key, files.get(0));
            } else {
                StringBuilder sb = new StringBuilder();
                for(File file: files) {
                    sb.append(file.getPath()).append(",");
                }
                int length = sb.toString().length();
                sb.deleteCharAt(length-1);
                LOG.info("using multipart upload for key " + key + ", block files: " + sb.toString());
                doMultipartPut(files, key);
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    public void storeEmptyFile(String key) throws IOException {
        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            objMeta.setContentLength(0);
            File dir = Utils.getTempBufferDir(conf);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IOException("Cannot create OSS buffer directory: " + dir);
            }
            File result = File.createTempFile("input-", ".empty", dir);
            ossClient.putObject(bucket, key, result);
            result.deleteOnExit();
        } catch (Exception e) {
            handleException(e);
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
        } catch (Exception e) {
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
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, key);
            OSSObject object = ossClient.getObject(bucket, key, 0, objectMetadata.getContentLength(), conf);
            return object.getObjectContent();
        } catch (Exception e) {
            handleException(key, e);
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
            OSSObject object = ossClient.getObject(bucket, key, byteRangeStart, fileSize-1, conf);
            return object.getObjectContent();
        } catch (Exception e) {
            handleException(key, e);
            return null; //never returned - keep compiler happy
        }
    }

    public InputStream retrieve(String key, long byteRangeStart, long length) throws IOException {
        try {
            if (!doesObjectExist(key)) {
                return null;
            }
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, key);
            long fileSize = objectMetadata.getContentLength();
            long end;
            if (fileSize - 1 >= byteRangeStart + length) {
                end = byteRangeStart + length;
            } else {
                end = fileSize - 1;
            }

            OSSObject object = ossClient.getObject(bucket, key, byteRangeStart, end, conf);
            return object.getObjectContent();
        } catch (Exception e) {
            handleException(key, e);
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

            ObjectListing listing = ossClient.listObjects(bucket, prefix, delimiter, maxListingLength, priorLastKey, conf);
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
        } catch (Exception e) {
            handleException(e);
            return null; //never returned - keep compiler happy
        }
    }

    public void delete(String key) throws IOException {
        try {
            ossClient.deleteObject(bucket, key);
        } catch (Exception e) {
            handleException(key, e);
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
        } catch (Exception e) {
            handleException(srcKey, e);
        }
    }

    public void purge(String prefix) throws IOException {
        try {
            List<OSSObjectSummary> objects = ossClient.listObjects(bucket, prefix).getObjectSummaries();
            for(OSSObjectSummary ossObjectSummary: objects) {
                ossClient.deleteObject(bucket, ossObjectSummary.getKey());
            }
        } catch (Exception e) {
            handleException(e);
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("OSS Native Filesystem, ");
        sb.append(bucket).append("\n");
        try {
            List<OSSObjectSummary> objects = ossClient.listObjects(bucket).getObjectSummaries();
            for(OSSObjectSummary ossObjectSummary: objects) {
                sb.append(ossObjectSummary.getKey()).append("\n");
            }
        } catch (Exception e) {
            handleException(e);
        }
        System.out.println(sb);
    }

    private void handleException(String key, Exception e) throws IOException, OssException {
        if (e instanceof ServiceException && "NoSuchKey".equals(((ServiceException) e).getErrorCode())) {
            throw new FileNotFoundException("Key '" + key + "' does not exist in OSS");
        } else {
            handleException(e);
        }
        LOG.error(e);
    }

    private void handleException(Exception e) throws IOException, OssException {
        if (e.getCause() instanceof IOException) {
            throw (IOException) e.getCause();
        } else {
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

    private void doMultipartCopy(String srcKey, String dstKey, Long contentLength) throws IOException {
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClient.initiateMultipartUpload(bucket, dstKey, conf);
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
                    ossClient, uploadId, bucket, bucket, srcKey, dstKey, size, skipBytes, i+1, conf);
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

        ossClient.completeMultipartUpload(bucket, dstKey, uploadId, partETags, conf);
    }

    private void doMultipartPut(File file, String key) throws IOException {
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClient.initiateMultipartUpload(bucket, key, conf);
        String uploadId = initiateMultipartUploadResult.getUploadId();

        Long contentLength = file.length();
        Long minSplitSize = contentLength / numSplitsUpperLimit + 1;
        Long partSize = Math.max(Math.min(maxSplitSize, contentLength / numSplits), minSplitSize);
        int partCount = (int) (contentLength / partSize);
        if (contentLength % partSize != 0) {
            partCount++;
        }
        LOG.info("multipart uploading, partCount" + partCount + ", partSize " + partSize);

        List<Task> tasks = new ArrayList<Task>();
        for (int i = 0; i < partCount; i++) {
            long skipBytes = partSize * i;
            long size = partSize < contentLength - skipBytes ? partSize : contentLength - skipBytes;
            OSSPutTask ossPutTask = new OSSPutTask(ossClient, uploadId, bucket, key, size, skipBytes, i+1, file, conf);
            ossPutTask.setUuid(i + "");
            tasks.add(ossPutTask);
        }

        List<PartETag> partETags = new ArrayList<PartETag>();
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
        ossClient.completeMultipartUpload(bucket, key, uploadId, partETags, conf);
    }

    private void doMultipartPut(List<File> files, String key) throws IOException {
        InitiateMultipartUploadResult initiateMultipartUploadResult =
                ossClient.initiateMultipartUpload(bucket, key, conf);
        String uploadId = initiateMultipartUploadResult.getUploadId();

        Long totalContentLength = 0L;
        for(File file: files) {
            totalContentLength += file.length();
        }
        Long minSplitSize = totalContentLength / numSplitsUpperLimit + 1;
        Long partSize = Math.max(Math.min(maxSplitSize, totalContentLength / numSplits), minSplitSize);
        int partCount = (int) (totalContentLength / partSize);
        if (totalContentLength % partSize != 0) {
            partCount++;
        }
        LOG.info("multipart uploading, partCount" + partCount + ", partSize " + partSize);

        List<Task> tasks = new ArrayList<Task>();
        int t = 0;
        for(File file: files) {
            Long contentLength = file.length();
            boolean _continue;
            int j = 0;
            long skipBytes;
            long size;
            do{
                skipBytes = partSize * j;
                if(partSize < contentLength - skipBytes) {
                    if(contentLength - (skipBytes + partSize) < 1024 * 1024) {
                        size = contentLength - skipBytes;
                        _continue = false;
                    } else {
                        size = partSize;
                        _continue = true;
                    }
                } else {
                    size = contentLength - skipBytes;
                    _continue = false;
                }
                OSSPutTask ossPutTask = new OSSPutTask(ossClient, uploadId, bucket, key, size, skipBytes, t+1, file, conf);
                ossPutTask.setUuid(t + "");
                tasks.add(ossPutTask);
                j++;
                t++;
            } while(_continue);
        }

        List<PartETag> partETags = new ArrayList<PartETag>();
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
        ossClient.completeMultipartUpload(bucket, key, uploadId, partETags, conf);
    }
}
