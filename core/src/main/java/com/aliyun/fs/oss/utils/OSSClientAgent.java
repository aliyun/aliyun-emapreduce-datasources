/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.fs.oss.utils;

import com.aliyun.ms.MetaClient;
import com.aliyun.ms.utils.EndpointEnum;
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.AppendObjectResult;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectResult;
import com.aliyun.oss.model.UploadPartCopyResult;
import com.aliyun.oss.model.UploadPartResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class OSSClientAgent {
  private static final Log LOG = LogFactory.getLog(OSSClientAgent.class);
  private static final String OSS_STS_SECURITY_TOKEN_EXPIRED_MESSAGE =
      "SecurityTokenExpired";
  private static final String OSS_INVALID_ACCESS_KEY_ID_MESSAGE =
      "InvalidAccessKeyId";
  private Object ossClient;
  private Class ossClientClz;
  private Gson gson = new Gson();
  private Configuration conf;

  @SuppressWarnings("unchecked")
  public OSSClientAgent(String endpoint, String accessKeyId,
      String accessKeySecret, Configuration conf) throws Exception {
    this.ossClientClz = ResourceLoader.getInstance().getUrlClassLoader(conf)
        .loadClass("com.aliyun.oss.OSSClient");
    Class ClientConfigurationClz = ResourceLoader.getInstance().getUrlClassLoader(conf)
        .loadClass("com.aliyun.oss.ClientConfiguration");
    Object clientConfiguration =
        initializeOSSClientConfig(conf, ClientConfigurationClz);
    Constructor cons = this.ossClientClz.getConstructor(String.class,
        String.class, String.class, ClientConfigurationClz);
    this.ossClient = cons.newInstance(endpoint, accessKeyId, accessKeySecret,
        clientConfiguration);
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  public OSSClientAgent(String endpoint, String accessKeyId,
      String accessKeySecret, String securityToken, Configuration conf)
      throws Exception {
    this.ossClientClz = ResourceLoader.getInstance().getUrlClassLoader(conf)
        .loadClass("com.aliyun.oss.OSSClient");
    Class ClientConfigurationClz = ResourceLoader.getInstance()
        .getUrlClassLoader(conf).loadClass("com.aliyun.oss.ClientConfiguration");
    Object clientConfiguration =
        initializeOSSClientConfig(conf, ClientConfigurationClz);
    Constructor cons = ossClientClz.getConstructor(String.class, String.class,
        String.class, String.class, ClientConfigurationClz);
    this.ossClient = cons.newInstance(endpoint, accessKeyId, accessKeySecret,
        securityToken, clientConfiguration);
    this.conf = conf;
  }

  @SuppressWarnings("unchecked")
  private boolean updateOSSClient(Exception e) {
    if (e instanceof InvocationTargetException) {
      Throwable t = ((InvocationTargetException) e).getTargetException();
      if (t.getMessage().contains(OSS_STS_SECURITY_TOKEN_EXPIRED_MESSAGE) ||
          t.getMessage().contains(OSS_INVALID_ACCESS_KEY_ID_MESSAGE)) {
        String accessKeyId = MetaClient.getRoleAccessKeyId();
        String accessKeySecret = MetaClient.getRoleAccessKeySecret();
        String securityToken = MetaClient.getRoleSecurityToken();
        String endpoint =
            EndpointEnum.getEndpoint("oss", MetaClient.getClusterRegionName(),
                MetaClient.getClusterNetworkType());
        try {
          Class ClientConfigurationClz = ResourceLoader.getInstance()
              .getUrlClassLoader(conf)
              .loadClass("com.aliyun.oss.ClientConfiguration");
          Object clientConfiguration =
              initializeOSSClientConfig(conf, ClientConfigurationClz);
          Constructor cons = ossClientClz.getConstructor(String.class,
              String.class, String.class, String.class, ClientConfigurationClz);
          this.ossClient = cons.newInstance(endpoint, accessKeyId,
              accessKeySecret, securityToken, clientConfiguration);
          return true;
        } catch (Exception e1) {
          return false;
        }
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  private PutObjectResult putObject(String bucket, String key, File file,
      boolean retry) throws IOException, ServiceException, ClientException {
    try {
      Method method = this.ossClientClz.getMethod("putObject", String.class,
          String.class, File.class);
      Object ret = method.invoke(this.ossClient, bucket, key, file);
      return gson.fromJson(gson.toJson(ret), PutObjectResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return putObject(bucket, key, file, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public PutObjectResult putObject(String bucket, String key, File file)
      throws IOException, ServiceException, ClientException {
    return putObject(bucket, key, file, true);
  }

  @SuppressWarnings("unchecked")
  private AppendObjectResult appendObject(String bucket, String key, File file,
       Long position, Configuration conf, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Class AppendObjectRequestClz = ResourceLoader.getInstance()
          .getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.AppendObjectRequest");
      Constructor cons = AppendObjectRequestClz.getConstructor(String.class,
          String.class, File.class);
      Object appendObjectRequest = cons.newInstance(bucket, key, file);
      Method method0 = AppendObjectRequestClz.getMethod("setPosition", Long.TYPE);
      method0.invoke(appendObjectRequest, position);
      Method method = this.ossClientClz
          .getMethod("appendObject", AppendObjectRequestClz);
      Object ret = method.invoke(this.ossClient, appendObjectRequest);
      return gson.fromJson(gson.toJson(ret), AppendObjectResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return appendObject(bucket, key, file, position, conf, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public AppendObjectResult appendObject(String bucket, String key, File file,
      Long position, Configuration conf)
      throws IOException, ServiceException, ClientException {
    return appendObject(bucket, key, file, position, conf, true);
  }

  @SuppressWarnings("unchecked")
  private ObjectMetadata getObjectMetadata(String bucket, String key,
      boolean retry) throws IOException, ServiceException, ClientException {
    try {
      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(ObjectMetadata.class,
          new ObjectMetadataDeserializer());
      Gson gson = builder.create();

      Method method = this.ossClientClz.getMethod("getObjectMetadata",
          String.class, String.class);
      Object ret = method.invoke(this.ossClient, bucket, key);
      return gson.fromJson(gson.toJson(ret), ObjectMetadata.class);
    } catch (NoSuchMethodException e) {
      LOG.error(e.getMessage());
      return null;
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return getObjectMetadata(bucket, key, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public ObjectMetadata getObjectMetadata(String bucket, String key)
      throws IOException, ServiceException, ClientException {
    return getObjectMetadata(bucket, key, true);
  }

  @SuppressWarnings("unchecked")
  private OSSObject getObject(String bucket, String key, long start, long end,
      Configuration conf, boolean retry)
      throws IOException, ServiceException, ClientException {
    InputStream inputStream;
    try {
      Class GetObjectRequestClz = ResourceLoader.getInstance()
          .getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.GetObjectRequest");
      Constructor cons0 =
          GetObjectRequestClz.getConstructor(String.class, String.class);
      Object getObjRequest = cons0.newInstance(bucket, key);
      Method method0 = GetObjectRequestClz.getMethod("setRange",
          Long.TYPE, Long.TYPE);
      method0.invoke(getObjRequest, start, end);

      Method method = this.ossClientClz.getMethod("getObject",
          GetObjectRequestClz);
      Object ret = method.invoke(this.ossClient, getObjRequest);

      Class OSSObjectClz = ResourceLoader.getInstance().getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.OSSObject");
      Method method1 = OSSObjectClz.getMethod("getObjectContent");
      inputStream = (InputStream) method1.invoke(ret);

      GsonBuilder builder = new GsonBuilder();
      builder.registerTypeAdapter(ObjectMetadata.class,
          new ObjectMetadataDeserializer());
      Gson gson = builder.create();
      Method method2 = OSSObjectClz.getMethod("getObjectMetadata");
      Object metadata = method2.invoke(ret);
      ObjectMetadata objectMetadata = gson.fromJson(gson.toJson(metadata),
          ObjectMetadata.class);

      OSSObject ossObject = new OSSObject();
      ossObject.setBucketName(bucket);
      ossObject.setKey(key);
      ossObject.setObjectContent(inputStream);
      ossObject.setObjectMetadata(objectMetadata);
      return ossObject;
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return getObject(bucket, key, start, end, conf, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public OSSObject getObject(String bucket, String key, long start,
      long end, Configuration conf)
      throws IOException, ServiceException, ClientException {
    return getObject(bucket, key, start, end, conf, true);
  }

  @SuppressWarnings("unchecked")
  private CopyObjectResult copyObject(String srcBucket, String srcKey,
      String dstBucket, String dstKey, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Method method = this.ossClientClz.getMethod("copyObject", String.class,
          String.class, String.class, String.class);
      Object ret =
          method.invoke(this.ossClient, srcBucket, srcKey, dstBucket, dstKey);
      return gson.fromJson(gson.toJson(ret), CopyObjectResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return copyObject(srcBucket, srcKey, dstBucket, dstKey, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public CopyObjectResult copyObject(String srcBucket, String srcKey,
      String dstBucket, String dstKey)
      throws IOException, ServiceException, ClientException {
    return copyObject(srcBucket, srcKey, dstBucket, dstKey, true);
  }

  @SuppressWarnings("unchecked")
  private ObjectListing listObjects(String bucket, boolean retry)
      throws IOException {
    try {
      Method method = this.ossClientClz.getMethod("listObjects", String.class);
      Object ret = method.invoke(this.ossClient, bucket);
      return gson.fromJson(gson.toJson(ret), ObjectListing.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return listObjects(bucket, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public ObjectListing listObjects(String bucket) throws IOException {
    return listObjects(bucket, true);
  }

  @SuppressWarnings("unchecked")
  private ObjectListing listObjects(String bucket, String prefix, boolean retry)
      throws IOException {
    try {
      Method method =
          this.ossClientClz.getMethod("listObjects", String.class, String.class);
      Object ret = method.invoke(this.ossClient, bucket, prefix);
      return gson.fromJson(gson.toJson(ret), ObjectListing.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return listObjects(bucket, prefix, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public ObjectListing listObjects(String bucket, String prefix)
      throws IOException {
    return listObjects(bucket, prefix, true);
  }

  @SuppressWarnings("unchecked")
  private ObjectListing listObjects(String bucket, String prefix,
      String delimiter, Integer maxListingLength, String priorLastKey,
      Configuration conf, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Class ListObjectsRequestClz = ResourceLoader.getInstance()
          .getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.ListObjectsRequest");
      Constructor cons = ListObjectsRequestClz.getConstructor(String.class);
      Object listObjectsRequest = cons.newInstance(bucket);
      Method method0 =
          ListObjectsRequestClz.getMethod("setDelimiter", String.class);
      method0.invoke(listObjectsRequest, delimiter);
      Method method1 =
          ListObjectsRequestClz.getMethod("setMarker", String.class);
      method1.invoke(listObjectsRequest, priorLastKey);
      Method method2 =
          ListObjectsRequestClz.getMethod("setMaxKeys", Integer.class);
      method2.invoke(listObjectsRequest, maxListingLength);
      Method method3 =
          ListObjectsRequestClz.getMethod("setPrefix", String.class);
      method3.invoke(listObjectsRequest, prefix);

      Method method =
          this.ossClientClz.getMethod("listObjects", ListObjectsRequestClz);
      Object ret = method.invoke(this.ossClient, listObjectsRequest);
      return gson.fromJson(gson.toJson(ret), ObjectListing.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return listObjects(bucket, prefix, delimiter, maxListingLength,
            priorLastKey, conf, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public ObjectListing listObjects(String bucket, String prefix,
      String delimiter, Integer maxListingLength, String priorLastKey,
      Configuration conf) throws IOException, ServiceException, ClientException {
    return listObjects(bucket, prefix, delimiter, maxListingLength,
        priorLastKey, conf, true);
  }

  @SuppressWarnings("unchecked")
  private void deleteObject(String bucket, String key, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Method method = this.ossClientClz.getMethod("deleteObject", String.class, String.class);
      method.invoke(this.ossClient, bucket, key);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        deleteObject(bucket, key, false);
      } else {
        handleException(e);
      }
    }
  }

  public void deleteObject(String bucket, String key)
      throws IOException, ServiceException, ClientException {
    deleteObject(bucket, key, true);
  }

  @SuppressWarnings("unchecked")
  private Boolean doesObjectExist(String bucket, String key, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Method method = this.ossClientClz.getMethod("doesObjectExist",
          String.class, String.class);
      Object ret = method.invoke(this.ossClient, bucket, key);
      return gson.fromJson(gson.toJson(ret), Boolean.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return doesObjectExist(bucket, key, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public Boolean doesObjectExist(String bucket, String key)
      throws IOException, ServiceException, ClientException {
    return doesObjectExist(bucket, key, true);
  }

  @SuppressWarnings("unchecked")
  private InitiateMultipartUploadResult initiateMultipartUpload(String bucket,
      String key, Configuration conf, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Class InitiateMultipartUploadRequestClz = ResourceLoader.getInstance().
          getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.InitiateMultipartUploadRequest");
      Constructor cons = InitiateMultipartUploadRequestClz
          .getConstructor(String.class, String.class);
      Object initiateMultipartUploadRequest = cons.newInstance(bucket, key);

      Method method = this.ossClientClz.getMethod("initiateMultipartUpload",
          InitiateMultipartUploadRequestClz);
      Object ret = method.invoke(this.ossClient, initiateMultipartUploadRequest);
      return gson.fromJson(gson.toJson(ret), InitiateMultipartUploadResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return initiateMultipartUpload(bucket, key, conf, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public InitiateMultipartUploadResult initiateMultipartUpload(String bucket,
      String key, Configuration conf)
      throws IOException, ServiceException, ClientException {
    return initiateMultipartUpload(bucket, key, conf, true);
  }

  @SuppressWarnings("unchecked")
  private void abortMultipartUpload(String bucket, String key, String uploadId,
      Configuration conf, boolean retry) throws IOException {
    try {
      Class AbortMultipartUploadRequestClz = ResourceLoader.getInstance().
          getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.AbortMultipartUploadRequest");
      Constructor cons = AbortMultipartUploadRequestClz
          .getConstructor(String.class, String.class, String.class);
      Object abortMultipartUploadRequest =
          cons.newInstance(bucket, key, uploadId);

      Method method = ossClientClz.getMethod("abortMultipartUpload",
          AbortMultipartUploadRequestClz);
      method.invoke(this.ossClient, abortMultipartUploadRequest);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        abortMultipartUpload(bucket, key, uploadId, conf, false);
      } else {
        handleException(e);
      }
    }
  }

  public void abortMultipartUpload(String bucket, String key, String uploadId,
      Configuration conf) throws IOException {
    abortMultipartUpload(bucket, key, uploadId, conf, true);
  }

  @SuppressWarnings("unchecked")
  private CompleteMultipartUploadResult completeMultipartUpload(String bucket,
      String key, String uploadId, List<PartETag> partETags, Configuration conf,
      boolean retry) throws IOException, ServiceException, ClientException {
    try {
      Class PartETagClz = ResourceLoader.getInstance().getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.PartETag");
      List<Object> tags = new ArrayList<Object>();
      for (PartETag partETag : partETags) {
        Constructor cons =
            PartETagClz.getConstructor(Integer.TYPE, String.class);
        Object tag =
            cons.newInstance(partETag.getPartNumber(), partETag.getETag());
        tags.add(tag);
      }

      Class CompleteMultipartUploadRequestClz = ResourceLoader.getInstance()
          .getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.CompleteMultipartUploadRequest");
      Constructor cons = CompleteMultipartUploadRequestClz
          .getConstructor(String.class, String.class, String.class, List.class);
      Object completeMultipartUploadRequest =
          cons.newInstance(bucket, key, uploadId, tags);

      Method method = this.ossClientClz.getMethod("completeMultipartUpload",
          CompleteMultipartUploadRequestClz);
      Object ret = method.invoke(this.ossClient, completeMultipartUploadRequest);
      return gson.fromJson(gson.toJson(ret), CompleteMultipartUploadResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return completeMultipartUpload(bucket, key, uploadId, partETags,
            conf, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public CompleteMultipartUploadResult completeMultipartUpload(String bucket,
      String key, String uploadId, List<PartETag> partETags, Configuration conf)
      throws IOException, ServiceException, ClientException {
    return completeMultipartUpload(bucket, key, uploadId, partETags, conf, true);
  }

  @SuppressWarnings("unchecked")
  private UploadPartResult uploadPart(String uploadId, String bucket, String key,
       Long partSize, Long beginIndex, int partNumber, File file,
       Configuration conf, boolean retry)
      throws IOException, ServiceException, ClientException {
    InputStream instream = null;
    try {
      instream = new FileInputStream(file);
      instream.skip(beginIndex);

      Class UploadPartRequestClz = ResourceLoader.getInstance()
          .getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.UploadPartRequest");
      Constructor cons = UploadPartRequestClz.getConstructor();
      Object uploadPartRequest = cons.newInstance();
      Method method0 =
          UploadPartRequestClz.getMethod("setBucketName", String.class);
      method0.invoke(uploadPartRequest, bucket);
      Method method1 =
          UploadPartRequestClz.getMethod("setKey", String.class);
      method1.invoke(uploadPartRequest, key);
      Method method2 =
          UploadPartRequestClz.getMethod("setUploadId", String.class);
      method2.invoke(uploadPartRequest, uploadId);
      Method method3 =
          UploadPartRequestClz.getMethod("setInputStream", InputStream.class);
      method3.invoke(uploadPartRequest, instream);
      Method method4 =
          UploadPartRequestClz.getMethod("setPartSize", Long.TYPE);
      method4.invoke(uploadPartRequest, partSize);
      Method method5 =
          UploadPartRequestClz.getMethod("setPartNumber", Integer.TYPE);
      method5.invoke(uploadPartRequest, partNumber);

      Method method =
          this.ossClientClz.getMethod("uploadPart", UploadPartRequestClz);
      Object ret = method.invoke(this.ossClient, uploadPartRequest);
      return gson.fromJson(gson.toJson(ret), UploadPartResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return uploadPart(uploadId, bucket, key, partSize, beginIndex,
            partNumber, file, conf, false);
      } else {
        handleException(e);
        return null;
      }
    } finally {
      if (instream != null) {
        try {
          instream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public UploadPartResult uploadPart(String uploadId,
                                     String bucket,
                                     String key,
                                     Long partSize,
                                     Long beginIndex,
                                     int partNumber,
                                     File file,
                                     Configuration conf) throws IOException, ServiceException, ClientException {
    return uploadPart(uploadId, bucket, key, partSize, beginIndex, partNumber, file, conf, true);
  }

  @SuppressWarnings("unchecked")
  private UploadPartCopyResult uploadPartCopy(String uploadId, String srcBucket,
       String dstBucket, String srcKey, String dstKey, Long partSize,
       Long beginIndex, int partNumber, Configuration conf, boolean retry)
      throws IOException, ServiceException, ClientException {
    try {
      Class UploadPartCopyRequestClz = ResourceLoader.getInstance()
          .getUrlClassLoader(conf)
          .loadClass("com.aliyun.oss.model.UploadPartCopyRequest");
      Constructor cons = UploadPartCopyRequestClz
          .getConstructor(String.class, String.class, String.class, String.class);
      Object uploadPartCopyRequest =
          cons.newInstance(srcBucket, srcKey, dstBucket, dstKey);
      Method method0 =
          UploadPartCopyRequestClz.getMethod("setBeginIndex", Long.class);
      method0.invoke(uploadPartCopyRequest, beginIndex);
      Method method1 =
          UploadPartCopyRequestClz.getMethod("setUploadId", String.class);
      method1.invoke(uploadPartCopyRequest, uploadId);
      Method method2 =
          UploadPartCopyRequestClz.getMethod("setPartSize", Long.class);
      method2.invoke(uploadPartCopyRequest, partSize);
      Method method3 =
          UploadPartCopyRequestClz.getMethod("setPartNumber", Integer.TYPE);
      method3.invoke(uploadPartCopyRequest, partNumber);

      Method method =
          this.ossClientClz.getMethod("uploadPartCopy", UploadPartCopyRequestClz);
      Object ret = method.invoke(this.ossClient, uploadPartCopyRequest);
      return gson.fromJson(gson.toJson(ret), UploadPartCopyResult.class);
    } catch (Exception e) {
      if (retry && updateOSSClient(e)) {
        return uploadPartCopy(uploadId, srcBucket, dstBucket, srcKey, dstKey,
            partSize, beginIndex, partNumber, conf, false);
      } else {
        handleException(e);
        return null;
      }
    }
  }

  public UploadPartCopyResult uploadPartCopy(String uploadId, String srcBucket,
      String dstBucket, String srcKey, String dstKey, Long partSize,
      Long beginIndex, int partNumber, Configuration conf)
      throws IOException, ServiceException, ClientException {
    return uploadPartCopy(uploadId, srcBucket, dstBucket, srcKey, dstKey,
        partSize, beginIndex, partNumber, conf, true);
  }

  @SuppressWarnings("unchecked")
  private Object initializeOSSClientConfig(Configuration conf, Class ClientConfigurationClz)
      throws IOException, ServiceException, ClientException {
    try {
      Constructor cons = ClientConfigurationClz.getConstructor();
      Object clientConfiguration = cons.newInstance();
      Method method0 =
          ClientConfigurationClz.getMethod("setConnectionTimeout", Integer.TYPE);
      method0.invoke(clientConfiguration,
          conf.getInt("fs.oss.client.connection.timeout",
              ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT));
      Method method1 =
          ClientConfigurationClz.getMethod("setSocketTimeout", Integer.TYPE);
      method1.invoke(clientConfiguration,
          conf.getInt("fs.oss.client.socket.timeout",
              ClientConfiguration.DEFAULT_SOCKET_TIMEOUT));
      Method method2 =
          ClientConfigurationClz.getMethod("setConnectionTTL", Long.TYPE);
      method2.invoke(clientConfiguration,
          conf.getLong("fs.oss.client.connection.ttl",
              ClientConfiguration.DEFAULT_CONNECTION_TTL));
      Method method3 =
          ClientConfigurationClz.getMethod("setMaxConnections", Integer.TYPE);
      method3.invoke(clientConfiguration,
          conf.getInt("fs.oss.connection.max",
              ClientConfiguration.DEFAULT_MAX_CONNECTIONS));

      return clientConfiguration;
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  private void handleException(Exception e)
      throws IOException, ServiceException, ClientException {
    if (e instanceof InvocationTargetException) {
      Throwable t = ((InvocationTargetException) e).getTargetException();
      if (t instanceof OSSException) {
        throw new OSSException(t.getMessage(), t.getCause());
      } else if (t instanceof ServiceException) {
        throw new ServiceException(t.getMessage(), t.getCause());
      } else if (t instanceof ClientException) {
        throw new ClientException(t.getMessage(), t.getCause());
      } else {
        throw new IOException(e);
      }
    } else {
      throw new IOException(e);
    }
  }

  private static class NaturalDeserializer implements JsonDeserializer<Object> {
    public Object deserialize(JsonElement json, Type typeOfT,
                              JsonDeserializationContext context) {
      if (json.isJsonNull()) return null;
      else if (json.isJsonPrimitive())
        return handlePrimitive(json.getAsJsonPrimitive());
      else if (json.isJsonArray())
        return handleArray(json.getAsJsonArray(), context);
      else return handleObject(json.getAsJsonObject(), context);
    }

    private Object handlePrimitive(JsonPrimitive json) {
      if (json.isBoolean())
        return json.getAsBoolean();
      else if (json.isString())
        return json.getAsString();
      else {
        BigDecimal bigDec = json.getAsBigDecimal();
        // Find out if it is an int type
        try {
          bigDec.toBigIntegerExact();
          try {
            return bigDec.intValueExact();
          } catch (ArithmeticException e) {
          }
          return bigDec.longValue();
        } catch (ArithmeticException e) {
        }
        // Just return it as a double
        return bigDec.doubleValue();
      }
    }

    private Object handleArray(JsonArray json,
        JsonDeserializationContext context) {
      Object[] array = new Object[json.size()];
      for (int i = 0; i < array.length; i++)
        array[i] = context.deserialize(json.get(i), Object.class);
      return array;
    }

    private Object handleObject(JsonObject json,
        JsonDeserializationContext context) {
      Map<String, Object> map = new HashMap<String, Object>();
      for (Map.Entry<String, JsonElement> entry : json.entrySet())
        map.put(entry.getKey(),
            context.deserialize(entry.getValue(), Object.class));
      return map;
    }
  }

  private static class ObjectMetadataDeserializer
      implements JsonDeserializer<ObjectMetadata> {
    private DateFormat df =
        new SimpleDateFormat("MMM d, yyyy K:mm:ss a", Locale.ENGLISH);

    public ObjectMetadata deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) {
      ObjectMetadata objectMetadata = new ObjectMetadata();
      JsonObject obj = json.getAsJsonObject();
      Iterator<Map.Entry<String, JsonElement>> it = obj.entrySet().iterator();
      Map.Entry<String, JsonElement> userMetadataEntry = it.next();
      if (userMetadataEntry == null) {
        return null;
      }

      Map<String, String> userMetadata = new HashMap<String, String>();
      JsonObject json0 = userMetadataEntry.getValue().getAsJsonObject();
      for (Map.Entry<String, JsonElement> entry0 : json0.entrySet()) {
        userMetadata.put(entry0.getKey(), entry0.getValue().getAsString());
      }
      objectMetadata.setUserMetadata(userMetadata);
      Map.Entry<String, JsonElement> metaDataEntry = it.next();
      JsonObject json1 = metaDataEntry.getValue().getAsJsonObject();
      Date date;
      Long l;
      for (Map.Entry<String, JsonElement> entry1 : json1.entrySet()) {
        String key = entry1.getKey();
        String value = entry1.getValue().getAsString();
        try {
          date = df.parse(value);
          objectMetadata.setHeader(key, date);
        } catch (ParseException e0) {
          try {
            l = Long.parseLong(value);
            objectMetadata.setHeader(key, l);
          } catch (NumberFormatException e1) {
            objectMetadata.setHeader(key, value);
          }
        }
      }
      return objectMetadata;
    }
  }
}
