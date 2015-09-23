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

package com.aliyun.fs.oss.common;

import com.aliyun.fs.oss.nat.NativeOssFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URI;
import java.util.*;

public class InMemoryNativeFileSystemStore implements NativeFileSystemStore {
    private Configuration conf;

    private SortedMap<String, FileMetadata> metadataMap =
            new TreeMap<String, FileMetadata>();
    private SortedMap<String, byte[]> dataMap = new TreeMap<String, byte[]>();

    public void initialize(URI uri, Configuration conf) throws IOException {
        this.conf = conf;
    }

    public void storeEmptyFile(String key) throws IOException {
        metadataMap.put(key, new FileMetadata(key, 0, System.currentTimeMillis()));
        dataMap.put(key, new byte[0]);
    }

    public boolean objectExists(String key) throws IOException {
        return false;
    }

    public void storeFile(String key, File file, boolean append)
            throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int numRead;
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            while ((numRead = in.read(buf)) >= 0) {
                out.write(buf, 0, numRead);
            }
        } finally {
            if (in != null) {
                in.close();
            }
        }
        metadataMap.put(key,
                new FileMetadata(key, file.length(), System.currentTimeMillis()));
        dataMap.put(key, out.toByteArray());
    }

    public InputStream retrieve(String key) throws IOException {
        return retrieve(key, 0);
    }

    public InputStream retrieve(String key, long byteRangeStart)
            throws IOException {

        byte[] data = dataMap.get(key);
        File file = createTempFile();
        BufferedOutputStream out = null;
        try {
            out = new BufferedOutputStream(new FileOutputStream(file));
            out.write(data, (int) byteRangeStart,
                    data.length - (int) byteRangeStart);
        } finally {
            if (out != null) {
                out.close();
            }
        }
        return new FileInputStream(file);
    }

    private File createTempFile() throws IOException {
        File dir = new File(conf.get("fs.oss.buffer.dir"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create OSS buffer directory: " + dir);
        }
        File result = File.createTempFile("test-", ".tmp", dir);
        result.deleteOnExit();
        return result;
    }

    public FileMetadata retrieveMetadata(String key) throws IOException {
        return metadataMap.get(key);
    }

    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {
        return list(prefix, maxListingLength, null, false);
    }

    public PartialListing list(String prefix, int maxListingLength,
                               String priorLastKey, boolean recursive) throws IOException {

        return list(prefix, recursive ? null : NativeOssFileSystem.PATH_DELIMITER, maxListingLength, priorLastKey);
    }

    private PartialListing list(String prefix, String delimiter,
                                int maxListingLength, String priorLastKey) throws IOException {

        if (prefix.length() > 0 && !prefix.endsWith(NativeOssFileSystem.PATH_DELIMITER)) {
            prefix += NativeOssFileSystem.PATH_DELIMITER;
        }

        List<FileMetadata> metadata = new ArrayList<FileMetadata>();
        SortedSet<String> commonPrefixes = new TreeSet<String>();
        for (String key : dataMap.keySet()) {
            if (key.startsWith(prefix)) {
                if (delimiter == null) {
                    metadata.add(retrieveMetadata(key));
                } else {
                    int delimIndex = key.indexOf(delimiter, prefix.length());
                    if (delimIndex == -1) {
                        metadata.add(retrieveMetadata(key));
                    } else {
                        String commonPrefix = key.substring(0, delimIndex);
                        commonPrefixes.add(commonPrefix);
                    }
                }
            }
            if (metadata.size() + commonPrefixes.size() == maxListingLength) {
                new PartialListing(key, metadata.toArray(new FileMetadata[0]),
                        commonPrefixes.toArray(new String[0]));
            }
        }
        return new PartialListing(null, metadata.toArray(new FileMetadata[0]),
                commonPrefixes.toArray(new String[0]));
    }

    public void delete(String key) throws IOException {
        metadataMap.remove(key);
        dataMap.remove(key);
    }

    public void copy(String srcKey, String dstKey) throws IOException {
        metadataMap.put(dstKey, metadataMap.get(srcKey));
        dataMap.put(dstKey, dataMap.get(srcKey));
    }

    public void purge(String prefix) throws IOException {
        Iterator<Map.Entry<String, FileMetadata>> i =
                metadataMap.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, FileMetadata> entry = i.next();
            if (entry.getKey().startsWith(prefix)) {
                dataMap.remove(entry.getKey());
                i.remove();
            }
        }
    }

    public void dump() throws IOException {
        System.out.println(metadataMap.values());
        System.out.println(dataMap.keySet());
    }
}
