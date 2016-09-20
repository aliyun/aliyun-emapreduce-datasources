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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Random;

public class Utils {
  static final Log LOG = LogFactory.getLog(Utils.class);
  static final String loginUser = System.getProperty("user.name");

  public static synchronized File getTempBufferDir(Configuration conf) {
    String[] dataDirs =
        conf.get("dfs.datanode.data.dir", "file:///tmp/").split(",");
    double maxUsage = Double.MIN_VALUE;
    int n = 0;
    for (int i = 0; i < dataDirs.length; i++) {
      File file = new File(new Path(dataDirs[i].trim()).toUri().getPath());
      double diskUsage = 1.0 * (file.getTotalSpace() - file.getFreeSpace())
          / file.getTotalSpace();
      if (diskUsage > maxUsage) {
        n = i;
        maxUsage = diskUsage;
      }
    }

    // skip the disk whose free space is most not enough, and pick one
    // randomly from the last disks.
    int idx;
    if (dataDirs.length == 1) {
      idx = 0;
    } else {
      while (true) {
        int i = Math.abs(new Random(System.currentTimeMillis()).nextInt())
            % dataDirs.length;
        if (i != n) {
          idx = i;
          break;
        }
      }
    }

    String diskPath = new Path(dataDirs[idx].trim()).toUri().getPath();
    LOG.debug("choose oss buffer dir: " + diskPath);
    return new File(diskPath, "data/" + loginUser + "/oss");
  }
}
