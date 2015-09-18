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

import com.aliyun.fs.oss.common.InMemoryNativeFileSystemStore;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class TestNativeOssFileSystem extends TestCase {
    private Configuration conf;
    private JetOssNativeFileSystemStore store;
    private NativeOssFileSystem fs;

    public void testInitialization() throws IOException {
        initializationTest("oss://a:b@c", "ossn://a:b@c");
        initializationTest("oss://a:b@c/", "ossn://a:b@c");
        initializationTest("oss://a:b@c/path", "ossn://a:b@c");
        initializationTest("oss://a@c", "ossn://a@c");
        initializationTest("oss://a@c/", "ossn://a@c");
        initializationTest("oss://a@c/path", "ossn://a@c");
        initializationTest("oss://c", "ossn://c");
        initializationTest("oss://c/", "ossn://c");
        initializationTest("oss://c/path", "ossn://c");
    }

    private void initializationTest(String initializationUri, String expectedUri)
            throws IOException {
        NativeOssFileSystem fs = new NativeOssFileSystem(new InMemoryNativeFileSystemStore());
        fs.initialize(URI.create(initializationUri), conf);
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
        store = new JetOssNativeFileSystemStore();
        fs = new NativeOssFileSystem(store);
        fs.initialize(URI.create("oss://bucket/"), conf);
    }

    @Override
    protected void tearDown() throws Exception {
        store.purge("test");
        super.tearDown();
    }

    private void createTestFiles(String base) throws IOException {
        store.storeEmptyFile(base + "/file1");
        store.storeEmptyFile(base + "/dir/file2");
        store.storeEmptyFile(base + "/dir/file3");
    }

    public void testDirWithDifferentMarkersWorks() throws Exception {
        for (int i = 0; i < 3; i++) {
            String base = "test/oss" + i;
            Path path = path(base);

            createTestFiles(base);

            if (i == 0) {
                // test for _$folder$ marker
                store.storeEmptyFile(base + "_$folder$");
                store.storeEmptyFile(base + "/dir_$folder$");
            } else if (i == 1) {
                // test the end slash file marker
                store.storeEmptyFile(base + "/");
                store.storeEmptyFile(base + "/dir/");
            }

            assertTrue(fs.getFileStatus(path).isDir());
            assertEquals(2, fs.listStatus(path).length);
        }
    }

    public void testDeleteWithNoMarker() throws IOException {
        String base = "test/oss";
        Path path = path(base);

        createTestFiles(base);

        fs.delete(path, true);

        assertNull(fs.listStatus(path));
    }

    public void testRenameWithNoMarker() throws IOException {
        String base = "test/oss";
        Path target = path("test/oss2");

        createTestFiles(base);

        fs.rename(path(base), target);

        Path path = path("test");
        assertTrue(fs.getFileStatus(path).isDir());
        assertEquals(1, fs.listStatus(path).length);
        assertTrue(fs.getFileStatus(target).isDir());
        assertEquals(2, fs.listStatus(target).length);
    }

    public void testEmptyFile() throws Exception {
        store.storeEmptyFile("test/oss/file1");
        fs.open(path("test/oss/file1")).close();
    }

    public void testAppendWrite() throws IOException {
        String base = "test/oss";
        Path path = path(base);

        FSDataOutputStream fsDataOutputStream = fs.append(path);
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
        fsDataInputStream.read(bytes);
        String content = new String(bytes);
        assert(content.equals("Hello world!"));
    }

    public void testEmptyDirectory() throws IOException {
        String base = "test";
        Path dir = path(base);

        fs.mkdirs(dir);
        FileStatus[] fileStatuses = fs.listStatus(dir);
        assert(fileStatuses.length == 0);

        FSDataOutputStream fsDataOutputStream = fs.create(path("test/file"));
        fsDataOutputStream.write("Hello World!".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        fileStatuses = fs.listStatus(dir);
        assert(fileStatuses.length == 1);
    }
}
