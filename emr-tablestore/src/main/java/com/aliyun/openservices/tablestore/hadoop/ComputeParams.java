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

package com.aliyun.openservices.tablestore.hadoop;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class ComputeParams implements Writable {
    private int maxSplitsCount;
    private long splitSizeInMbs;
    private String computeMode;

    public ComputeParams() {
    }

    public ComputeParams(int maxSplitsCount, long splitSizeInMbs, String computeMode) {
        this.maxSplitsCount = maxSplitsCount;
        this.splitSizeInMbs = splitSizeInMbs;
        this.computeMode = computeMode;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(WritableConsts.COMPUTE_PARAMETERS);
        out.writeInt(maxSplitsCount);
        out.writeLong(splitSizeInMbs);
        out.writeUTF(computeMode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.COMPUTE_PARAMETERS) {
            throw new IOException("broken input stream");
        }
        maxSplitsCount = in.readInt();
        splitSizeInMbs = in.readLong();
        computeMode = in.readUTF();
    }

    public static ComputeParams read(DataInput in) throws IOException {
        ComputeParams params = new ComputeParams();
        params.readFields(in);
        return params;
    }

    public String serialize() {
        return Utils.serialize(this);
    }

    public static ComputeParams deserialize(String in) {
        if (in == null) {
            return null;
        }
        byte[] buf = Base64.decodeBase64(in);
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(is);
        try {
            return read(din);
        } catch (IOException ex) {
            return null;
        }
    }

    public int getMaxSplitsCount() {
        return maxSplitsCount;
    }

    public long getSplitSizeInMbs() {
        return splitSizeInMbs;
    }

    public String getComputeMode() {
        return computeMode;
    }
}
