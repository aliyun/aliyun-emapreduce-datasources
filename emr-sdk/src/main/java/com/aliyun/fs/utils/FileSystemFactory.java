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

package com.aliyun.fs.utils;

import com.aliyun.fs.oss.blk.OssFileSystem;
import com.aliyun.fs.oss.common.PrimitiveFileSystem;
import com.aliyun.fs.oss.nat.NativeOssFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class FileSystemFactory {

    public static PrimitiveFileSystem get(Path path, Configuration conf) throws IOException {
        URI uri = path.toUri();
        String schema = uri.getScheme();

        if (schema.equals("oss"))
            return new OssFileSystem();
        else if (schema.equals("ossn"))
            return new NativeOssFileSystem();
        else
            throw new RuntimeException("This SDK dose not support other Path `schema`, except `oss://` or `ossn://`");
    }

    public static boolean checkBlockBased(Path path) {
        URI uri = path.toUri();
        String schema = uri.getScheme();

        if (schema.equals("oss"))
            return true;
        else
            return false;
    }
}
