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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;

public class Utils {
    private static Log LOG = LogFactory.getLog(Utils.class);
    public static synchronized File getTempBufferDir(Configuration conf) {
        String[] dataDirs = conf.get("dfs.datanode.data.dir", "file:///tmp/").split(",");
        double minUsage = Double.MAX_VALUE;
        int n = 0;
        for(int i=0; i<dataDirs.length; i++) {
            File file = new File(new Path(dataDirs[i]).toUri().getPath());
            double diskUsage = 1.0 * (file.getTotalSpace() - file.getFreeSpace()) / file.getTotalSpace();
            if (diskUsage < minUsage) {
                n = i;
                minUsage = diskUsage;
            }
            i++;
        }

        String diskPath = new Path(dataDirs[n]).toUri().getPath();
        LOG.debug("choose oss buffer dir: "+diskPath+", and this disk usage is: "+minUsage);
        return new File(diskPath, "data/oss");
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.data.dir", "file:///mnt/disk1,file:///mnt/disk4");
        System.out.println(Utils.getTempBufferDir(conf));
    }
}
