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

package com.aliyun.fs.oss.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class OssRecordReader implements RecordReader<LongWritable, Text> {

  private static final Log LOG
      = LogFactory.getLog(OssRecordReader.class.getName());
  private final Seekable filePosition;
  int maxLineLength;
  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private FSDataInputStream fileIn;
  private CompressionCodec codec;
  private Decompressor decompressor;

  public OssRecordReader(Configuration job, FileSplit split, FileSystem fs,
                         byte[] recordDelimiter) throws IOException {
    this.maxLineLength = job.getInt(org.apache.hadoop.mapreduce.lib.input.
        LineRecordReader.MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    fileIn = fs.open(file);
    if (isCompressedInput()) {
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        final SplitCompressionInputStream cIn =
            ((SplittableCompressionCodec) codec).createInputStream(
                fileIn, decompressor, start, end,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new LineReader(cIn, job, recordDelimiter);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        filePosition = cIn; // take pos from compressed stream
      } else {
        in = new LineReader(codec.createInputStream(fileIn, decompressor), job,
            recordDelimiter);
        filePosition = fileIn;
      }
    } else {
      fileIn.seek(start);
      in = new LineReader(fileIn, job, recordDelimiter);
      filePosition = fileIn;
    }
    // If this is not the first split, we always throw away first record
    // because we always (except the last split) read one extra line in
    // next() method.
    if (start != 0) {
      start += in.readLine(new Text(), 0, maxBytesToConsume(start));
    }
    this.pos = start;
  }

  private boolean isCompressedInput() {
    return (codec != null);
  }

  private int maxBytesToConsume(long pos) {
    return isCompressedInput()
        ? Integer.MAX_VALUE
        : (int) Math.min(Integer.MAX_VALUE, end - pos);
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput() && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  public boolean next(LongWritable key, Text value) throws IOException {
    // We always read one extra line, which lies outside the upper
    // split limit i.e. (end - 1)
    while (getFilePosition() <= end) {
      key.set(pos);

      int newSize = in.readLine(value, maxLineLength,
          Math.max(maxBytesToConsume(pos), maxLineLength));
      if (newSize == 0) {
        return false;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        return true;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    return false;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }
  }

  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
    }
  }
}
