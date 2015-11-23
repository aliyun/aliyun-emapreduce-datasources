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
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.Bucket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Utils {
    public static final Log LOG = LogFactory.getLog(Utils.class);
    public static File getOSSBufferDir(Configuration conf) {
        boolean confirmExists = conf.getBoolean("fs.oss.buffer.dirs.exists", false);
        String[] bufferDirs = conf.get("fs.oss.buffer.dirs", "file:///tmp/").split(",");
        List<String> bufferPaths = new ArrayList<String>();
        for(int i = 0; i < bufferDirs.length; i++) {
            URI uri = new Path(bufferDirs[i]).toUri();
            String path = uri.getPath();
            Boolean fileExists = new File(path).exists();
            if (confirmExists && !fileExists) {
                continue;
            }
            bufferPaths.add(path);
        }
        if (bufferPaths.size() == 0) {
            bufferPaths.add("/tmp/");
        }
        int randomIdx = (new Random()).nextInt() % bufferPaths.size();
        return new File(bufferPaths.get(Math.abs(randomIdx)), "oss");
    }

    public static String getEndpoint(String bucket, String accessKeyId, String accessKeySecret) throws IOException {
        for(EndpointEnum endpoint: EndpointEnum.getInternalEndpoints()) {
            try {
                OSSClient client = new OSSClient(endpoint.getLocation(), accessKeyId, accessKeySecret);
                List<Bucket> buckets = client.listBuckets();
                for (Bucket bucket1 : buckets) {
                    if (bucket1.getName().equals(bucket)) {
                        return endpoint.getLocation();
                    }
                }
            } catch (ServiceException e) {
                if (e.getErrorCode().equals("ConnectionTimeout")) {
                    LOG.debug("current bucket dose not exist in " + endpoint.getName() + ", skip it.", e);
                } else {
                    throw new IOException("Cannot find appropriate endpoint for bucket " + bucket);
                }
            }
        }
        throw new IOException("Cannot find OSS bucket " + bucket + " , specify an existing bucket please!");
    }
}
