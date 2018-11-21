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

package com.aliyun.fs.oss.blk;

import com.aliyun.fs.oss.common.Block;
import com.aliyun.fs.oss.common.FileSystemStore;
import com.aliyun.fs.oss.common.INode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;

@Deprecated
public class OssAppendOutputStream extends OssOutputStream {

  public OssAppendOutputStream(Configuration conf, FileSystemStore store,
      Path path, INode inode, long blockSize, Progressable progress,
      int bufferSize, ArrayList<Block> blocksForOneTime) throws IOException {
    super(conf, store, path, blockSize, progress, bufferSize, blocksForOneTime);

    Block[] blocks = inode.getBlocks();
    for (Block block : blocks) {
      this.blocks.add(block);
    }
  }
}
