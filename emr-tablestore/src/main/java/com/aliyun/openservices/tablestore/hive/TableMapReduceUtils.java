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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Borrowed from https://github.com/apache/hbase/blob/master/hbase-mapreduce/src/main/java/org/apache/hadoop/hbase/mapreduce/TableMapReduceUtil.java
 */
public class TableMapReduceUtils {
    private static final Log LOG = LogFactory.getLog(TableMapReduceUtils.class);

  public static void addDependencyJars(Job job) throws IOException {
    try {
      addDependencyJarsForClasses(job.getConfiguration(), job.getMapOutputKeyClass(),
          job.getMapOutputValueClass(), job.getInputFormatClass(), job.getOutputKeyClass(),
          job.getOutputValueClass(), job.getOutputFormatClass(), job.getPartitionerClass(),
          job.getCombinerClass());
    } catch (ClassNotFoundException var2) {
      throw new IOException(var2);
    }
  }

  public static void addDependencyJarsForClasses(Configuration conf, Class... classes)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet();
    jars.addAll(conf.getStringCollection("tmpjars"));
    Map<String, String> packagedClasses = new HashMap();
    Class[] var5 = classes;
    int var6 = classes.length;

    for(int var7 = 0; var7 < var6; ++var7) {
      Class<?> clazz = var5[var7];
      if (clazz != null) {
        Path path = findOrCreateJar(clazz, localFs, packagedClasses);
        if (path == null) {
          LOG.warn(
              "Could not find jar for class " + clazz + " in order to ship it to the cluster.");
        } else if (!localFs.exists(path)) {
          LOG.warn("Could not validate jar file " + path + " for class " + clazz);
        } else {
          jars.add(path.toString());
        }
      }
    }

    if (!jars.isEmpty()) {
      conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
    }
  }

  private static Path findOrCreateJar(
      Class<?> my_class, FileSystem fs, Map<String, String> packagedClasses)
      throws IOException {
    String jar = findContainingJar(my_class, packagedClasses);
    if (null == jar || jar.isEmpty()) {
      jar = getJar(my_class);
      updateMap(jar, packagedClasses);
    }

    if (null != jar && !jar.isEmpty()) {
      LOG.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
      return (new Path(jar)).makeQualified(fs);
    } else {
      return null;
    }
  }

  private static String findContainingJar(
      Class<?> my_class, Map<String, String> packagedClasses)
      throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    String class_file =
        my_class.getName().replaceAll("\\.", "/") + ".class";
    if (loader != null) {
      Enumeration itr = loader.getResources(class_file);

      while(itr.hasMoreElements()) {
        URL url = (URL)itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }

          toReturn = toReturn.replaceAll("\\+", "%2B");
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    }

    return (String)packagedClasses.get(class_file);
  }

  private static String getJar(Class<?> my_class) {
    String ret = null;

    try {
      ret = JarFinder.getJar(my_class);
      return ret;
    } catch (Exception var3) {
      throw new RuntimeException("getJar invocation failed.", var3);
    }
  }

  private static void updateMap(String jar, Map<String, String> packagedClasses)
      throws IOException {
    if (null != jar && !jar.isEmpty()) {
      ZipFile zip = null;

      try {
        zip = new ZipFile(jar);
        Enumeration iter = zip.entries();

        while(iter.hasMoreElements()) {
          ZipEntry entry = (ZipEntry)iter.nextElement();
          if (entry.getName().endsWith("class")) {
            packagedClasses.put(entry.getName(), jar);
          }
        }
      } finally {
        if (null != zip) {
          zip.close();
        }

      }

    }
  }
}
