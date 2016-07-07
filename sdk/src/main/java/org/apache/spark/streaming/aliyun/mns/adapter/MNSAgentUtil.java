/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.streaming.aliyun.mns.adapter;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

public class MNSAgentUtil {
    private static final Log LOG = LogFactory.getLog(MNSAgentUtil.class);
    private volatile static URLClassLoader urlClassLoader;

    public static MNSClientAgent getMNSClientAgent(String accessKeyId,
                                                   String accessKeySecret,
                                                   String endpoint) throws Exception {
        Configuration conf = new Configuration();
        return getMNSClientAgent(accessKeyId, accessKeySecret, endpoint, conf);
    }

    @SuppressWarnings("unchecked")
    public static MNSClientAgent getMNSClientAgent(String accessKeyId,
                                                   String accessKeySecret,
                                                   String endpoint,
                                                   Configuration conf) throws Exception {
        Class cloudAccountClz = getUrlClassLoader(conf).loadClass("com.aliyun.mns.client.CloudAccount");
        Class mnsClientClz = getUrlClassLoader(conf).loadClass("com.aliyun.mns.client.MNSClient");
        Constructor cons = cloudAccountClz.getConstructor(String.class, String.class, String.class);
        Object cloudAccount = cons.newInstance(accessKeyId, accessKeySecret, endpoint);
        Method method = cloudAccountClz.getMethod("getMNSClient");
        Object mnsClient = method.invoke(cloudAccount);
        return new MNSClientAgent(mnsClient, mnsClientClz, urlClassLoader);
    }

    @SuppressWarnings("unchecked")
    private static URLClassLoader getUrlClassLoader(Configuration conf){
        if(urlClassLoader == null){
            synchronized(MNSAgentUtil.class){
                if(urlClassLoader == null){
                    try {
                        String[] internalDep = getMNSClasses(conf);
                        ArrayList<URL> urls = new ArrayList<URL>();
                        if (internalDep != null) {
                            for(String dep: internalDep) {
                                urls.add(new URL("file://" + dep));
                            }
                        }
                        String[] cp;
                        if (SystemUtils.IS_OS_WINDOWS) {
                            cp = System.getProperty("java.class.path").split(";");
                            for (String entity : cp) {
                                urls.add(new URL("file:" + entity));
                            }
                        } else {
                            cp = System.getProperty("java.class.path").split(":");
                            for (String entity : cp) {
                                urls.add(new URL("file://" + entity));
                            }
                        }
                        urlClassLoader = new URLClassLoader(urls.toArray(new URL[0]), null);
                    } catch (Exception e) {
                        throw new RuntimeException("Can not initialize MNS URLClassLoader, " + e.getMessage());
                    }
                }
            }
        }
        return urlClassLoader;
    }

    private static String[] getMNSClasses(Configuration conf) throws Exception {
        String deps = conf.get("fs.oss.sdk.dependency.path");
        Boolean runLocal = conf.getBoolean("job.runlocal", false);
        if ((deps == null || deps.isEmpty()) && !runLocal) {
            throw new RuntimeException("Job dose not run locally, set 'fs.oss.sdk.dependency.path' first please.");
        } else if (deps == null || deps.isEmpty()) {
            LOG.info("'job.runlocal' set true.");
            return null;
        } else {
            return deps.split(",");
        }
    }
}
