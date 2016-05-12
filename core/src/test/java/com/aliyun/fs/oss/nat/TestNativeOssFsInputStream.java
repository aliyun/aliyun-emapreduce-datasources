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

    public void testFlushCacheWithOutException() throws IOException {
        InputStream in = mock(InputStream.class);
        String key = "uttest/a.txt";
        FSDataOutputStream fsDataOutputStream = fs.create(path(key));
        fsDataOutputStream.write("Hello".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();

        NativeOssFileSystem.NativeOssFsInputStream nativeIn = fs.new NativeOssFsInputStream(key);
        nativeIn.readBuffer = new byte[2];

        assertEquals(2, nativeIn.flushReadCache());
        assertEquals(2, nativeIn.globalPos);
        assertEquals(0, nativeIn.pos);
        assertEquals(0, nativeIn.cacheIdx);
        assertEquals(2, nativeIn.cacheSize);
        assertEquals('H', nativeIn.readBuffer[0]);
        assertEquals('e', nativeIn.readBuffer[1]);

        nativeIn.pos = 2;
        assertEquals(2, nativeIn.flushReadCache());
        assertEquals(4, nativeIn.globalPos);
        assertEquals(2, nativeIn.pos);
        assertEquals(0, nativeIn.cacheIdx);
        assertEquals(2, nativeIn.cacheSize);
        assertEquals('l', nativeIn.readBuffer[0]);
        assertEquals('l', nativeIn.readBuffer[1]);

        nativeIn.pos = 4;
        assertEquals(1, nativeIn.flushReadCache());
        assertEquals(5, nativeIn.globalPos);
        assertEquals(4, nativeIn.pos);
        assertEquals(0, nativeIn.cacheIdx);
        assertEquals(1, nativeIn.cacheSize);
        assertEquals('o', nativeIn.readBuffer[0]);
    }

    public void testFlushCacheWithException() throws IOException {
        InputStream in0 = new ByteArrayInputStream("".getBytes());
        InputStream in1 = new ByteArrayInputStream("test flushCache with Exception!!!!".getBytes());
        InputStream in2 = mock(InputStream.class);
        InputStream in3 = mock(InputStream.class);
        InputStream in4 = mock(InputStream.class);

        FileMetadata metadata = mock(FileMetadata.class);
        JetOssNativeFileSystemStore store = mock(JetOssNativeFileSystemStore.class);
        String key = "uttest/a.txt";
        when(store.retrieve(key, 0, 68157440)).thenReturn(in1);
        when(store.retrieve(key, 32, 68157440)).thenReturn(in2);
        when(store.retrieve(key, 62, 68157440)).thenReturn(in3);
        when(store.retrieve(key, 48, 68157440)).thenReturn(in4);
        when(metadata.getLength()).thenReturn(30L);
        when(store.retrieveMetadata(key)).thenReturn(metadata);

        NativeOssFileSystem fs = new NativeOssFileSystem(store);
        fs.initialize(URI.create("oss://emr/"), conf);
        NativeOssFileSystem.NativeOssFsInputStream nativeIn = fs.new NativeOssFsInputStream(key);
        nativeIn.readBuffer = new byte[32];

        NativeOssFileSystem.NativeOssFsInputStream spyNativeIn = spy(nativeIn);
        Mockito.doNothing().when(spyNativeIn).closeInnerStream();
        when(in2.read(spyNativeIn.readBuffer, 0, 32))
                .thenThrow(new IOException("read with exception"))
                .thenReturn(30);
        when(in2.read(spyNativeIn.readBuffer, 30, 2))
                .thenReturn(-1);
        when(in3.read(spyNativeIn.readBuffer, 0, 32))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenReturn(30);
        when(in3.read(spyNativeIn.readBuffer, 30, 2))
                .thenReturn(-1);

        when(in4.read(spyNativeIn.readBuffer, 0, 32))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenThrow(new IOException("read with exception"))
                .thenReturn(20);
        when(in4.read(spyNativeIn.readBuffer, 20, 12))
                .thenReturn(5);
        when(in4.read(spyNativeIn.readBuffer, 25, 7))
                .thenReturn(-1);

        assertEquals(32, spyNativeIn.flushReadCache());
        assertEquals(32, spyNativeIn.globalPos);
        assertEquals(0, spyNativeIn.pos);
        assertEquals(0, spyNativeIn.cacheIdx);
        assertEquals(32, spyNativeIn.cacheSize);

        spyNativeIn.pos = 32;
        assertEquals(30, spyNativeIn.flushReadCache());
        assertEquals(62, spyNativeIn.globalPos);
        assertEquals(32, spyNativeIn.pos);
        assertEquals(0, spyNativeIn.cacheIdx);
        assertEquals(30, spyNativeIn.cacheSize);

        spyNativeIn.pos = 62;
        try {
            spyNativeIn.flushReadCache();
            assertEquals(false, true);
        } catch (IOException e) {
            assertEquals(true, true);
        }

        spyNativeIn.pos = 48;
        assertEquals(25, spyNativeIn.flushReadCache());
        assertEquals(73, spyNativeIn.globalPos);
        assertEquals(48, spyNativeIn.pos);
        assertEquals(0, spyNativeIn.cacheIdx);
        assertEquals(25, spyNativeIn.cacheSize);

        spyNativeIn.in = null;
        Mockito.doNothing().when(spyNativeIn).updateInnerStream(in4, 48);
        try {
            spyNativeIn.flushReadCache();
            assertEquals(false, true);
        } catch (Exception e) {
            assertEquals(true, true);
        }
    }

    public void testRead() throws IOException {
        InputStream in = new ByteArrayInputStream("read from inputstream.".getBytes());

        String key = "uttest/a.txt";
        FileMetadata metadata = mock(FileMetadata.class);
        JetOssNativeFileSystemStore store = mock(JetOssNativeFileSystemStore.class);
        when(store.retrieve(key, 0, 68157440)).thenReturn(in);
        when(metadata.getLength()).thenReturn(22L);
        when(store.retrieveMetadata(key)).thenReturn(metadata);

        NativeOssFileSystem fs = new NativeOssFileSystem(store);
        fs.initialize(URI.create("oss://emr/"), conf);
        NativeOssFileSystem.NativeOssFsInputStream nativeIn = fs.new NativeOssFsInputStream(key);
        nativeIn.readBuffer = new byte[32];

        NativeOssFileSystem.NativeOssFsInputStream spyNativeIn = spy(nativeIn);
        Mockito.doNothing().when(spyNativeIn).closeInnerStream();
        spyNativeIn.readBuffer ="test read!!!".getBytes();
        Mockito.doReturn(-1).when(spyNativeIn).flushReadCache();
        spyNativeIn.pos = 0;
        spyNativeIn.globalPos = 0;
        assertEquals(-1, spyNativeIn.read());

        spyNativeIn.globalPos = 12;
        spyNativeIn.cacheIdx = 0;
        spyNativeIn.cacheSize = 12;

        assertEquals('t', spyNativeIn.read());
        assertEquals(1, spyNativeIn.pos);
        assertEquals(1, spyNativeIn.cacheIdx);
        assertEquals('e', spyNativeIn.read());
        assertEquals(2, spyNativeIn.pos);
        assertEquals(2, spyNativeIn.cacheIdx);
        while(spyNativeIn.read() > 0) {
        }
        assertEquals(12, spyNativeIn.pos);
        assertEquals(12, spyNativeIn.cacheIdx);

        spyNativeIn.readBuffer = new byte[32];
        Mockito.doCallRealMethod().when(spyNativeIn).flushReadCache();
        spyNativeIn.pos = 0;
        spyNativeIn.globalPos = 0;
        assertNotNull(in);
        assertEquals('r', spyNativeIn.read());
        assertEquals(1, spyNativeIn.pos);
        assertEquals(1, spyNativeIn.cacheIdx);
        assertEquals(22, spyNativeIn.globalPos);
        assertEquals('e', spyNativeIn.read());
        assertEquals(2, spyNativeIn.pos);
        assertEquals(2, spyNativeIn.cacheIdx);
        while(spyNativeIn.read() > 0) {
        }
        assertEquals(22, spyNativeIn.pos);
        assertEquals(22, spyNativeIn.cacheIdx);
        assertEquals(-1, spyNativeIn.read());
    }

    public void testReadRange() throws IOException {
        InputStream in = new ByteArrayInputStream("read from inputstream.".getBytes());

        String key = "uttest/a.txt";
        FileMetadata metadata = mock(FileMetadata.class);
        JetOssNativeFileSystemStore store = mock(JetOssNativeFileSystemStore.class);
        when(store.retrieve(key, 0, 68157440)).thenReturn(in);
        when(metadata.getLength()).thenReturn(22L);
        when(store.retrieveMetadata(key)).thenReturn(metadata);

        NativeOssFileSystem fs = new NativeOssFileSystem(store);
        fs.initialize(URI.create("oss://emr/"), conf);
        NativeOssFileSystem.NativeOssFsInputStream nativeIn = fs.new NativeOssFsInputStream(key);
        nativeIn.readBuffer = new byte[32];

        NativeOssFileSystem.NativeOssFsInputStream spyNativeIn = spy(nativeIn);
        Mockito.doNothing().when(spyNativeIn).closeInnerStream();

        byte[] buf = new byte[4];
        try {
            spyNativeIn.read(null, 0, 0);
            assertEquals(false, true);
        } catch (NullPointerException e) {
            assertEquals(true, true);
        }

        try {
            spyNativeIn.read(buf, -1, 2);
            assertEquals(false, true);
        } catch (IndexOutOfBoundsException e) {
            assertEquals(true, true);
        }

        try {
            spyNativeIn.read(buf, 1, -1);
            assertEquals(false, true);
        } catch (IndexOutOfBoundsException e) {
            assertEquals(true, true);
        }

        try {
            spyNativeIn.read(buf, 3, 2);
            assertEquals(false, true);
        } catch (IndexOutOfBoundsException e) {
            assertEquals(true, true);
        }

        Mockito.doReturn(-1).when(spyNativeIn).flushReadCache();
        spyNativeIn.pos = 0;
        spyNativeIn.globalPos = 0;
        assertEquals(-1, spyNativeIn.read(buf, 0, 4));

        Mockito.doCallRealMethod().when(spyNativeIn).flushReadCache();
        spyNativeIn.globalPos = 2;
        assertEquals(3, spyNativeIn.read(buf, 0, 3));
        assertEquals(3, spyNativeIn.cacheIdx);
        assertEquals(22, spyNativeIn.cacheSize);
        assertEquals('r', buf[0]);
        assertEquals('e', buf[1]);
        assertEquals('a', buf[2]);
        assertEquals(22, spyNativeIn.globalPos);
        assertEquals(3, spyNativeIn.pos);

        assertEquals(1, spyNativeIn.read(buf, 3, 1));
        assertEquals('d', buf[3]);
        assertEquals(22, spyNativeIn.globalPos);
        assertEquals(4, spyNativeIn.pos);
        assertEquals(4, spyNativeIn.cacheIdx);
        assertEquals(22, spyNativeIn.cacheSize);

        assertEquals(1, spyNativeIn.read(buf, 0, 1));
        assertEquals(32, buf[0]);

        assertEquals(4, spyNativeIn.read(buf, 0, 4));
        assertEquals("from", new String(buf));

        assertEquals(1, spyNativeIn.read(buf, 0, 1));
        assertEquals(32, buf[0]);

        assertEquals(4, spyNativeIn.read(buf, 0, 4));
        assertEquals("inpu", new String(buf));

        assertEquals(4, spyNativeIn.read(buf, 0, 4));
        assertEquals("tstr", new String(buf));

        assertEquals(4, spyNativeIn.read(buf, 0, 4));
        assertEquals("eam.", new String(buf));

        Mockito.doReturn(-1).when(spyNativeIn).flushReadCache();
        assertEquals(22, spyNativeIn.pos);
        assertEquals(22, spyNativeIn.globalPos);
        assertEquals(-1, spyNativeIn.read(buf, 0, 4));
    }
}
