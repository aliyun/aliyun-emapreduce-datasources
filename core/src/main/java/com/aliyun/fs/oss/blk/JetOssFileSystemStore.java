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

import java.io.*;
import java.net.URI;
import java.util.*;

import com.aliyun.fs.oss.common.*;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class JetOssFileSystemStore implements FileSystemStore {

    public static final Log LOG =
            LogFactory.getLog(JetOssFileSystemStore.class);

    private static final String FILE_SYSTEM_NAME = "fs";
    private static final String FILE_SYSTEM_VALUE = "Hadoop";

    private static final String FILE_SYSTEM_TYPE_NAME = "fs-type";
    private static final String FILE_SYSTEM_TYPE_VALUE = "block";

    private static final String FILE_SYSTEM_VERSION_NAME = "fs-version";
    private static final String FILE_SYSTEM_VERSION_VALUE = "1";

    private static final Map<String, String> METADATA =
            new HashMap<String, String>();

    static {
        METADATA.put(FILE_SYSTEM_NAME, FILE_SYSTEM_VALUE);
        METADATA.put(FILE_SYSTEM_TYPE_NAME, FILE_SYSTEM_TYPE_VALUE);
        METADATA.put(FILE_SYSTEM_VERSION_NAME, FILE_SYSTEM_VERSION_VALUE);
    }

    private static final String PATH_DELIMITER = Path.SEPARATOR;
    private static final String BLOCK_PREFIX = "block_data/block_";

    private Configuration conf;

    private OSSClient ossClient;

    private String bucket;

    private int bufferSize;

    public JetOssFileSystemStore() {

    }

    public JetOssFileSystemStore(OSSClient ossClient, String bucket, Configuration conf) {
        this.conf = conf;
        this.ossClient = ossClient;
        this.bucket = bucket;
        this.bufferSize = conf.getInt("io.file.buffer.size", 4096);
    }

    public void initialize(URI uri, Configuration conf) throws IOException {
        this.conf = conf;
        String endpoint = conf.get("fs.oss.endpoint");
        String accessKeyId = conf.get("fs.oss.accessKeyId");
        String accessKeySecret = conf.get("fs.oss.accessKeySecret");
        String securityToken = conf.get("fs.oss.securityToken");
        if (securityToken.equals("null")) {
            this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret);
        } else {
            this.ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret, securityToken);
        }
        this.bucket = uri.getHost();
        this.bufferSize = conf.getInt("io.file.buffer.size", 4096);
    }

    public String getVersion() throws IOException {
        return FILE_SYSTEM_VERSION_VALUE;
    }

    private void delete(String key) throws IOException {
        try {
            ossClient.deleteObject(bucket, key);
        } catch (ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    public void deleteINode(Path path) throws IOException {
        delete(pathToKey(path));
    }

    public void deleteBlock(Block block) throws IOException {
        delete(blockToKey(block));
    }

    public boolean inodeExists(Path path) throws IOException {
        InputStream in = get(pathToKey(path), true);
        if (in == null) {
            return false;
        }
        in.close();
        return true;
    }

    public boolean blockExists(long blockId) throws IOException {
        InputStream in = get(blockToKey(blockId), false);
        if (in == null) {
            return false;
        }
        in.close();
        return true;
    }

    private InputStream get(String key, boolean checkMetadata)
            throws IOException {
        try {
            OSSObject object = ossClient.getObject(bucket, key);
            if (checkMetadata) {
                checkMetadata(object);
            }
            return object.getObjectContent();
        } catch (ServiceException e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                return null;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    private InputStream get(String key, long byteRangeStart) throws IOException {
        try {
            ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucket, key);
            long fileSize = objectMetadata.getContentLength();
            GetObjectRequest getObjReq = new GetObjectRequest(bucket, key);
            getObjReq.setRange(byteRangeStart, fileSize - 1);
            OSSObject object = ossClient.getObject(getObjReq);
            return object.getObjectContent();
        } catch (ServiceException e) {
            if ("NoSuchKey".equals(e.getErrorCode())) {
                return null;
            }
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    private void checkMetadata(OSSObject object) throws OssFileSystemException,
            ServiceException {

        String name = object.getObjectMetadata().getUserMetadata().get(FILE_SYSTEM_NAME);
        if (!FILE_SYSTEM_VALUE.equals(name)) {
            throw new OssFileSystemException("Not a Hadoop OSS file.");
        }
        String type = object.getObjectMetadata().getUserMetadata().get(FILE_SYSTEM_TYPE_NAME);
        if (!FILE_SYSTEM_TYPE_VALUE.equals(type)) {
            throw new OssFileSystemException("Not a block file.");
        }
        String dataVersion = object.getObjectMetadata().getUserMetadata().get(FILE_SYSTEM_VERSION_NAME);
        if (!FILE_SYSTEM_VERSION_VALUE.equals(dataVersion)) {
            throw new VersionMismatchException(FILE_SYSTEM_VERSION_VALUE,
                    dataVersion);
        }
    }

    public INode retrieveINode(Path path) throws IOException {
        return INode.deserialize(get(pathToKey(path), true));
    }

    public File retrieveBlock(Block block, long byteRangeStart)
            throws IOException {
        File fileBlock = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            fileBlock = newBackupFile();
            in = get(blockToKey(block), byteRangeStart);
            out = new BufferedOutputStream(new FileOutputStream(fileBlock));
            byte[] buf = new byte[bufferSize];
            int numRead;
            while ((numRead = in.read(buf)) >= 0) {
                out.write(buf, 0, numRead);
            }
            return fileBlock;
        } catch (IOException e) {
            // close output stream to file then delete file
            closeQuietly(out);
            out = null; // to prevent a second close
            if (fileBlock != null) {
                fileBlock.delete();
            }
            throw e;
        } finally {
            closeQuietly(out);
            closeQuietly(in);
        }
    }

    private File newBackupFile() throws IOException {
        File dir = new File(conf.get("fs.oss.buffer.dir", "/tmp/oss/"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Cannot create OSS buffer directory: " + dir);
        }
        File result = File.createTempFile("input-", ".tmp", dir);
        result.deleteOnExit();
        return result;
    }

    public Set<Path> listSubPaths(Path path) throws IOException {
        try {
            List<OSSObjectSummary> ossObjectSummaries = new ArrayList<OSSObjectSummary>();

            String priorLastKey = null;
            do {
                String prefix = pathToKey(path);
                if (!prefix.endsWith(PATH_DELIMITER)) {
                    prefix += PATH_DELIMITER;
                }

                ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);
                listObjectsRequest.setPrefix(prefix);
                listObjectsRequest.setMarker(priorLastKey);
                listObjectsRequest.setDelimiter(PATH_DELIMITER);
                listObjectsRequest.setMaxKeys(OssFileSystem.OSS_MAX_LISTING_LENGTH);
                ObjectListing listing = ossClient.listObjects(listObjectsRequest);
                List<OSSObjectSummary> objects = listing.getObjectSummaries();
                Iterator<OSSObjectSummary> iter = objects.iterator();
                while (iter.hasNext()) {
                    OSSObjectSummary obj = iter.next();
                    ossObjectSummaries.add(obj);
                }
                priorLastKey = listing.getNextMarker();
            } while (priorLastKey != null);

            Set<Path> prefixes = new TreeSet<Path>();
            Iterator<OSSObjectSummary> iter = ossObjectSummaries.iterator();
            while(iter.hasNext()) {
                OSSObjectSummary obj = iter.next();
                prefixes.add(keyToPath(obj.getKey()));
            }
            prefixes.remove(path);
            return prefixes;
        } catch (ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    public Set<Path> listDeepSubPaths(Path path) throws IOException {
        try {
            String prefix = pathToKey(path);
            if (!prefix.endsWith(PATH_DELIMITER)) {
                prefix += PATH_DELIMITER;
            }
            OSSObject[] objects = (OSSObject[]) ossClient.listObjects(bucket, prefix).getObjectSummaries().toArray();
            Set<Path> prefixes = new TreeSet<Path>();
            for (int i = 0; i < objects.length; i++) {
                prefixes.add(keyToPath(objects[i].getKey()));
            }
            prefixes.remove(path);
            return prefixes;
        } catch (ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    private void put(String key, InputStream in, long length, boolean storeMetadata)
            throws IOException {

        try {
            ObjectMetadata objMeta = new ObjectMetadata();
            objMeta.setContentLength(length);
            if (storeMetadata) {
                objMeta.setUserMetadata(METADATA);
            }
            ossClient.putObject(bucket, key, in, objMeta);
        } catch (ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    public void storeINode(Path path, INode inode) throws IOException {
        put(pathToKey(path), inode.serialize(), inode.getSerializedLength(), true);
    }

    public void storeBlock(Block block, File file) throws IOException {
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            put(blockToKey(block), in, block.getLength(), false);
        } finally {
            closeQuietly(in);
        }
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public static String pathToKey(Path path) {
        if (path.isAbsolute()) {
            LOG.info("OSS File Path can not start with \"/\", so we need to scratch the first \"/\".");
            String absolutePath = path.toUri().getPath();
            return absolutePath.substring(1);
        }
        return path.toUri().getPath();
    }

    private Path keyToPath(String key) {
        return new Path(key);
    }

    private String blockToKey(long blockId) {
        return BLOCK_PREFIX + blockId;
    }

    private String blockToKey(Block block) {
        return blockToKey(block.getId());
    }

    public void purge() throws IOException {
        try {
            OSSObject[] objects = (OSSObject[]) ossClient.listObjects(bucket).getObjectSummaries().toArray();
            for (int i = 0; i < objects.length; i++) {
                ossClient.deleteObject(bucket, objects[i].getKey());
            }
        } catch (ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("OSS Filesystem, ");
        sb.append(bucket).append("\n");
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket);
            listObjectsRequest.setDelimiter(PATH_DELIMITER);
            OSSObject[] objects = (OSSObject[]) ossClient.listObjects(listObjectsRequest).getObjectSummaries().toArray();
            for (int i = 0; i < objects.length; i++) {
                Path path = keyToPath(objects[i].getKey());
                sb.append(path).append("\n");
                INode m = retrieveINode(path);
                sb.append("\t").append(m.getFileType()).append("\n");
                if (m.getFileType() == INode.FileType.DIRECTORY) {
                    continue;
                }
                for (int j = 0; j < m.getBlocks().length; j++) {
                    sb.append("\t").append(m.getBlocks()[j]).append("\n");
                }
            }
        } catch (ServiceException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new OssException(e);
        }
        System.out.println(sb);
    }
}
