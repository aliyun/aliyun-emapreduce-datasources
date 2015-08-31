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

package com.aliyun.fs.oss.blk;

import com.aliyun.fs.oss.common.InMemoryFileSystemStore;
import junit.framework.TestCase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class TestOssFileSystem extends TestCase {
    private Configuration conf;
    private JetOssFileSystemStore store;
    private OssFileSystem fs;

    public void testInitialization() throws IOException {
        initializationTest("oss://a:b@c", "oss://a:b@c");
        initializationTest("oss://a:b@c/", "oss://a:b@c");
        initializationTest("oss://a:b@c/path", "oss://a:b@c");
        initializationTest("oss://a@c", "oss://a@c");
        initializationTest("oss://a@c/", "oss://a@c");
        initializationTest("oss://a@c/path", "oss://a@c");
        initializationTest("oss://c", "oss://c");
        initializationTest("oss://c/", "oss://c");
        initializationTest("oss://c/path", "oss://c");
    }

    private void initializationTest(String initializationUri, String expectedUri)
            throws IOException {
        OssFileSystem fs = new OssFileSystem(new InMemoryFileSystemStore());
        fs.initialize(URI.create(initializationUri), new Configuration());
        assertEquals(URI.create(expectedUri), fs.getUri());
    }

    private Path path(String pathString) {
        return new Path("oss://bucket/" + pathString).makeQualified(fs);
    }

    @Override
    protected void setUp() throws IOException {
        conf = new Configuration();
        conf.set("fs.oss.endpoint", "endpointUrl");
        conf.set("fs.oss.accessKeyId", "accessKeyId");
        conf.set("fs.oss.accessKeySecret", "accessKeySecret");
        store = new JetOssFileSystemStore();
        fs = new OssFileSystem(store);
        fs.initialize(URI.create("oss://bucket/"), conf);
    }

    @Override
    protected void tearDown() throws Exception {
        fs.delete(path("test"));
        super.tearDown();
    }

    public void testAppendWrite() throws IOException {
        String base = "test/oss";
        Path path = path(base);

        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.write("Hello".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        Long fileLen = fs.getFileStatus(path).getLen();
        assert(fileLen == 5);

        fsDataOutputStream = fs.append(path);
        fsDataOutputStream.write(" world!".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        fileLen = fs.getFileStatus(path).getLen();
        assert(fileLen == 12);

        FSDataInputStream fsDataInputStream = fs.open(path);
        byte[] bytes = new byte[12];
        int numBytes = fsDataInputStream.read(bytes);
        while(numBytes < bytes.length) {
            numBytes += fsDataInputStream.read(bytes, numBytes, bytes.length);
        }
        String content = new String(bytes);
        assert(content.equals("Hello world!"));
    }
}
