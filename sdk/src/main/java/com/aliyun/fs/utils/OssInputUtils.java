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

import com.aliyun.fs.oss.common.OssRecordReader;
import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;

public class OssInputUtils {
    private Configuration conf;
    private FileSystem fs;

    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    public static final Log LOG =
            LogFactory.getLog(OssInputUtils.class);

    public OssInputUtils(Configuration conf) {
        this.conf = conf;
    }

    public FileSplit[] getSplits(String file, int numSplits) throws IOException {
        Path path = new Path(file);
        this.fs = FileSystem.get(path.toUri(), conf);
        fs.initialize(path.toUri(), conf);

        FileStatus[] files = fs.listStatus(path);
        long totalSize = 0;
        for(FileStatus file1: files) {
            if (file1.isDirectory()) {
                throw new IOException("Not a file: " + file1.getPath());
            }
            totalSize += file1.getLen();
        }

        long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
        long minSize = Math.max(conf.getLong(org.apache.hadoop.mapreduce.lib.input.
                FileInputFormat.SPLIT_MINSIZE, 1), 1);

        ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
        for (FileStatus file2: files) {
            Path fp = file2.getPath();
            long length = file2.getLen();
            if (length !=0) {
                long splitSize = Math.max(minSize, goalSize);
                long bytesRemaining = length;
                while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                    FileSplit split = new FileSplit(fp, length - bytesRemaining, splitSize, new String[0]);
                    splits.add(split);
                    bytesRemaining -= splitSize;
                }
                if (bytesRemaining != 0) {
                    FileSplit split = new FileSplit(fp, length - bytesRemaining, bytesRemaining, new String[0]);
                    splits.add(split);
                }
            }
        }
        LOG.info("Total # of splits: " + splits.size());
        return splits.toArray(new FileSplit[splits.size()]);
    }

    public RecordReader<LongWritable, Text> getOssRecordReader(FileSplit fileSplit, Configuration conf) throws IOException {
        String delimiter = conf.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }

        if (fs == null) {
            this.fs = FileSystem.get(fileSplit.getPath().toUri(), conf);
            fs.initialize(fileSplit.getPath().toUri(), conf);
        }

        return new OssRecordReader(conf, fileSplit, fs, recordDelimiterBytes);
    }

}
