/*
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

package com.aliyun.fs.oss;

import com.aliyun.fs.oss.nat.NativeOssFileSystem;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.internal.AssumptionViolatedException;

import java.io.IOException;
import java.net.URI;

/**
 * Utility class for Aliyun OSS Tests.
 */
public final class AliyunOSSTestUtils {

  private AliyunOSSTestUtils() {
  }

  /**
   * Create the test filesystem.
   *
   * If the test.fs.oss.name property is not set,
   * tests will fail.
   *
   * @param conf configuration
   * @return the FS
   * @throws IOException
   */
  public static NativeOssFileSystem createTestFileSystem(Configuration conf)
      throws IOException {
    String fsname = conf.getTrimmed(TestAliyunOSSFileSystemContract.TEST_FS_OSS_NAME);
    if (StringUtils.isEmpty(fsname)) {
      fsname = System.getenv("TEST_FS_OSS_NAME");
    }

    boolean liveTest = StringUtils.isNotEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals("oss");
    }

    if (!liveTest) {
      throw new AssumptionViolatedException("No test filesystem in "
          + TestAliyunOSSFileSystemContract.TEST_FS_OSS_NAME);
    }
    conf.set("mapreduce.job.run-local", "true");
    conf.set("fs.oss.buffer.dirs", "/tmp");

    conf = updateConfig(conf);

    NativeOssFileSystem ossfs = new NativeOssFileSystem();
    ossfs.initialize(testURI, conf);
    return ossfs;
  }

  /**
   * Generate unique test path for multiple user tests.
   *
   * @return root test path
   */
  public static String generateUniqueTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? "/test" :
        "/" + testUniqueForkId + "/test";
  }

  public static Configuration updateConfig(Configuration conf) {
    String accessKeyId = System.getenv("ALIYUN_ACCESS_KEY_ID");
    String accessKeySecret = System.getenv("ALIYUN_ACCESS_KEY_SECRET");
    String envType = System.getenv("TEST_ENV_TYPE");
    String region = System.getenv("REGION_NAME");
    if (accessKeyId != null) {
      conf.set("fs.oss.accessKeyId", accessKeyId);
    }
    if (accessKeySecret != null) {
      conf.set("fs.oss.accessKeySecret", accessKeySecret);
    }
    if (region == null) {
      region = "cn-hangzhou";
    }
    if (envType == null || (!envType.equals("private") && !envType.equals("public"))) {
      envType = "public";
    }
    if (envType.equals("public")) {
      conf.set("fs.oss.endpoint", "oss-" + region + ".aliyuncs.com");
    } else {
      conf.set("fs.oss.endpoint", "oss-" + region + "-internal.aliyuncs.com");
    }

    return conf;
  }
}
