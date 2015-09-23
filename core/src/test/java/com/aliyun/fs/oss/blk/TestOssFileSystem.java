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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class TestOssFileSystem extends TestCase {
    private Configuration conf;
    private JetOssFileSystemStore store;
    private OssFileSystem fs;

    public void testInitialization() throws IOException {
        initializationTest("ossbfs://a:b@c", "ossbfs://a:b@c");
        initializationTest("ossbfs://a:b@c/", "ossbfs://a:b@c");
        initializationTest("ossbfs://a:b@c/path", "ossbfs://a:b@c");
        initializationTest("ossbfs://a@c", "ossbfs://a@c");
        initializationTest("ossbfs://a@c/", "ossbfs://a@c");
        initializationTest("ossbfs://a@c/path", "ossbfs://a@c");
        initializationTest("ossbfs://c", "ossbfs://c");
        initializationTest("ossbfs://c/", "ossbfs://c");
        initializationTest("ossbfs://c/path", "ossbfs://c");
    }

    private void initializationTest(String initializationUri, String expectedUri)
            throws IOException {
        OssFileSystem fs = new OssFileSystem(new InMemoryFileSystemStore());
        fs.initialize(URI.create(initializationUri), new Configuration());
        assertEquals(URI.create(expectedUri), fs.getUri());
    }

    private Path path(String pathString) {
        return new Path("ossbfs://bucket/" + pathString).makeQualified(fs);
    }

    @Override
    protected void setUp() throws IOException {
        conf = new Configuration();
        conf.set("fs.oss.endpoint", "endpointUrl");
        conf.set("fs.oss.accessKeyId", "accessKeyId");
        conf.set("fs.oss.accessKeySecret", "accessKeySecret");
        store = new JetOssFileSystemStore();
        fs = new OssFileSystem(store);
        fs.initialize(URI.create("ossbfs://bucket/"), conf);
    }

    @Override
    protected void tearDown() throws Exception {
        fs.delete(path("test"));
        super.tearDown();
    }

    public void testAppendWrite() throws IOException {
        String base = "test/oss";
        Path path = path(base);

        createEmptyFile(path);

        Long fileLen = fs.getFileStatus(path).getLen();
        assert(fileLen == 5);

        FSDataOutputStream fsDataOutputStream = fs.append(path);
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

    public void testRename() throws IOException {
        Path path = path("test/dir/file1");
        Path renamed = path("test/dir/file2");
        createEmptyFile(path);

        fs.rename(path, renamed);
        assert(fs.exists(renamed));
        assert(!fs.exists(path));
    }

    public void testListStatus() throws IOException {
        String key0 = "test/file0";
        String key1 = "test/dir1/file1";
        String key2 = "test/dir1/dir12/file2";
        String key3 = "test/dir2/file3";
        createEmptyFile(path(key0));
        createEmptyFile(path(key1));
        createEmptyFile(path(key2));
        createEmptyFile(path(key3));

        assert(fs.listStatus(path("test")).length == 3);
        assert(fs.listStatus(path("test/file0")).length == 1);
        assert(fs.listStatus(path("test/dir1")).length == 2);
        assert(fs.listStatus(path("test/dir1/file1")).length == 1);
        assert(fs.listStatus(path("test/dir1/dir12/file2")).length == 1);
        assert(fs.listStatus(path("test/dir2")).length == 1);
        assert(fs.listStatus(path("test/dir2/file3")).length == 1);
        assert(fs.listStatus(path("test/dir3")).length == 0);

        assert(fs.listStatus(path("test/dir1/file1"))[0].isFile());
        assert(fs.listStatus(path("test/dir2"))[0].isFile());
        assert(fs.listStatus(path("test/dir1"))[0].isFile() && fs.listStatus(path("test/dir1"))[1].isDirectory()
                || fs.listStatus(path("test/dir1"))[1].isFile() && fs.listStatus(path("test/dir1"))[0].isDirectory());
    }

    public void testDelete() throws IOException {
        String key0 = "test/file0";
        String key1 = "test/dir1/file1";
        String key2 = "test/dir1/dir12/file2";
        createEmptyFile(path(key0));
        createEmptyFile(path(key1));
        createEmptyFile(path(key2));

        assert(!fs.delete(path("test2")));
        try {
            fs.delete(path("test"), false);
            assert(1 == 0);
        } catch (Exception e) {
            assert(1 == 1);
        }

        assert(fs.delete(path("test/file0")));
        assert(fs.delete(path("test/dir1")));
    }

    public void testFileStatus() throws IOException {
        createEmptyFile(path("test/dir1/file1"));

        assert(fs.getFileStatus(path("test/dir1/file1")).isFile());
        assert(fs.getFileStatus(path("test/dir1")).isDirectory());
        assert(fs.getFileStatus(path("/")).isDirectory());
        try {
            fs.getFileStatus(path("test2"));
            assert(1 == 0);
        } catch (Exception e) {
            assert(1 == 1);
        }
    }

    private void createEmptyFile(Path path) throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.write("Hello".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }
}
