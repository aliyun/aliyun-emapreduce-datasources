/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.openservices.tablestore.hive;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * Borrowed from https://github.com/apache/hbase/blob/master/hbase-mapreduce/src/main/java/org/apache/hadoop/hbase/mapreduce/JarFinder.java
 */
public class JarFinder {
  public JarFinder() {
  }

  private static void copyToZipStream(File file, ZipEntry entry, ZipOutputStream zos)
      throws IOException {
    FileInputStream is = new FileInputStream(file);

    try {
      zos.putNextEntry(entry);
      byte[] arr = new byte[4096];

      for(int read = is.read(arr); read > -1; read = is.read(arr)) {
        zos.write(arr, 0, read);
      }
    } finally {
      try {
        is.close();
      } finally {
        zos.closeEntry();
      }
    }

  }

  public static void jarDir(File dir, String relativePath, ZipOutputStream zos) throws IOException {
    Preconditions.checkNotNull(relativePath, "relativePath");
    Preconditions.checkNotNull(zos, "zos");
    File manifestFile = new File(dir, "META-INF/MANIFEST.MF");
    ZipEntry manifestEntry = new ZipEntry("META-INF/MANIFEST.MF");
    if (!manifestFile.exists()) {
      zos.putNextEntry(manifestEntry);
      (new Manifest()).write(new BufferedOutputStream(zos));
      zos.closeEntry();
    } else {
      copyToZipStream(manifestFile, manifestEntry, zos);
    }

    zos.closeEntry();
    zipDir(dir, relativePath, zos, true);
    zos.close();
  }

  private static void zipDir(File dir, String relativePath, ZipOutputStream zos, boolean start)
      throws IOException {
    String[] dirList = dir.list();
    if (dirList != null) {
      String[] var5 = dirList;
      int var6 = dirList.length;

      for(int var7 = 0; var7 < var6; ++var7) {
        String aDirList = var5[var7];
        File f = new File(dir, aDirList);
        if (!f.isHidden()) {
          String filePath;
          if (f.isDirectory()) {
            if (!start) {
              ZipEntry dirEntry = new ZipEntry(relativePath + f.getName() + "/");
              zos.putNextEntry(dirEntry);
              zos.closeEntry();
            }

            filePath = f.getPath();
            File file = new File(filePath);
            zipDir(file, relativePath + f.getName() + "/", zos, false);
          } else {
            filePath = relativePath + f.getName();
            if (!filePath.equals("META-INF/MANIFEST.MF")) {
              ZipEntry anEntry = new ZipEntry(filePath);
              copyToZipStream(f, anEntry, zos);
            }
          }
        }
      }

    }
  }

  private static void createJar(File dir, File jarFile) throws IOException {
    Preconditions.checkNotNull(dir, "dir");
    Preconditions.checkNotNull(jarFile, "jarFile");
    File jarDir = jarFile.getParentFile();
    if (!jarDir.exists() && !jarDir.mkdirs()) {
      throw new IOException(MessageFormat.format("could not create dir [{0}]", jarDir));
    } else {
      FileOutputStream fos = new FileOutputStream(jarFile);
      Throwable var4 = null;

      try {
        JarOutputStream jos = new JarOutputStream(fos);
        Throwable var6 = null;

        try {
          jarDir(dir, "", jos);
        } catch (Throwable var29) {
          var6 = var29;
          throw var29;
        } finally {
          if (jos != null) {
            if (var6 != null) {
              try {
                jos.close();
              } catch (Throwable var28) {
                var6.addSuppressed(var28);
              }
            } else {
              jos.close();
            }
          }

        }
      } catch (Throwable var31) {
        var4 = var31;
        throw var31;
      } finally {
        if (fos != null) {
          if (var4 != null) {
            try {
              fos.close();
            } catch (Throwable var27) {
              var4.addSuppressed(var27);
            }
          } else {
            fos.close();
          }
        }

      }

    }
  }

  public static String getJar(Class klass) {
    Preconditions.checkNotNull(klass, "klass");
    ClassLoader loader = klass.getClassLoader();
    if (loader != null) {
      String class_file = klass.getName().replaceAll("\\.", "/") + ".class";

      try {
        Enumeration itr = loader.getResources(class_file);

        while(itr.hasMoreElements()) {
          URL url = (URL)itr.nextElement();
          String path = url.getPath();
          if (path.startsWith("file:")) {
            path = path.substring("file:".length());
          }

          path = URLDecoder.decode(path, "UTF-8");
          if ("jar".equals(url.getProtocol())) {
            path = URLDecoder.decode(path, "UTF-8");
            return path.replaceAll("!.*$", "");
          }

          if ("file".equals(url.getProtocol())) {
            String klassName = klass.getName();
            klassName = klassName.replace(".", "/") + ".class";
            path = path.substring(0, path.length() - klassName.length());
            File baseDir = new File(path);
            File testDir = new File(System.getProperty("test.build.dir", "target/test-dir"));
            testDir = testDir.getAbsoluteFile();
            if (!testDir.exists()) {
              testDir.mkdirs();
            }

            File tempJar = File.createTempFile("hadoop-", "", testDir);
            tempJar = new File(tempJar.getAbsolutePath() + ".jar");
            tempJar.deleteOnExit();
            createJar(baseDir, tempJar);
            return tempJar.getAbsolutePath();
          }
        }
      } catch (IOException var10) {
        throw new RuntimeException(var10);
      }
    }

    return null;
  }
}
