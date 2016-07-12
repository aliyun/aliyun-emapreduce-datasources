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
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestNativeOssFsInputStream extends TestCase {
    private Configuration conf;
    private JetOssNativeFileSystemStore store;
    private NativeOssFileSystem fs;

    @Override
    protected void setUp() throws IOException {
        conf = new Configuration();
        conf.set("job.runlocal", "true");
        conf.set("fs.oss.endpoint", "endpoint");
        conf.set("fs.oss.accessKeyId", "accessKeyId");
        conf.set("fs.oss.accessKeySecret", "accessKeySecret");
        store = new JetOssNativeFileSystemStore();
        fs = new NativeOssFileSystem(store);
        fs.initialize(URI.create("oss://emr/"), conf);
    }

    @Override
    protected void tearDown() throws Exception {
        store.purge("uttest");
        super.tearDown();
    }

    private Path path(String pathString) {
        return new Path("oss://emr/" + pathString).makeQualified(fs);
    }
}
