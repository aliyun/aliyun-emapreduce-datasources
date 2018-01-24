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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;

import java.io.File;
import java.io.IOException;

public class Utils {
  static final Log LOG = LogFactory.getLog(Utils.class);
  static final String loginUser = System.getProperty("user.name");
  static final String BUFFER_DIR_KEY = "fs.oss.buffer.dirs";
  static final LocalDirAllocator dirAlloc =
      new LocalDirAllocator(BUFFER_DIR_KEY);

  public static synchronized File getTempBufferDir(Configuration conf)
      throws IOException {
    File tmpFile = dirAlloc.createTmpFileForWrite("output-",
        LocalDirAllocator.SIZE_UNKNOWN, conf);
    String dir = tmpFile.toPath().getParent().toUri().getPath();
    LOG.debug("choose oss buffer dir: " + dir);
    return new File(dir, "data/" + loginUser + "/oss");
  }

  /**
   * Calculate a proper size of multipart piece. If <code>minPartSize</code>
   * is too small, the number of multipart pieces may exceed the limit of
   * 10000.
   *
   * @param contentLength the size of file.
   * @param minPartSize the minimum size of multipart piece.
   * @return a revisional size of multipart piece.
   */
  public static long calculatePartSize(long contentLength, long minPartSize) {
    long tmpPartSize = contentLength / 10000 + 1;
    return Math.max(minPartSize, tmpPartSize);
  }

  /**
   * Check if OSS object represents a directory.
   *
   * @param name object key
   * @param size object content length
   * @return true if object represents a directory
   */
  public static boolean objectRepresentsDirectory(final String name,
      final long size) {
    return StringUtils.isNotEmpty(name) && name.endsWith("/") && size == 0L;
  }
}
