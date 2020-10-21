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
    private static final String COMPUTE_MODE_KV = "KV";
    private static final String COMPUTE_MODE_SEARCH = "Search";
    private static final String COMPUTE_MODE_AUTO = "Auto";

    private int maxSplitsCount;
    private long splitSizeInMBs;
    private String computeMode;

    private String searchIndexName = "";

    public ComputeParams() {
    }

    public ComputeParams(int maxSplitsCount, long splitSizeInMBs, String computeMode) {
        this.maxSplitsCount = maxSplitsCount;
        this.splitSizeInMBs = splitSizeInMBs;
        this.computeMode = computeMode;
    }

    public ComputeParams(String searchIndexName) {
        this.computeMode = COMPUTE_MODE_SEARCH;
        this.searchIndexName = searchIndexName;
    }

    public ComputeParams(String searchIndexName, int maxSplitsCount) {
        this.computeMode = COMPUTE_MODE_SEARCH;
        this.searchIndexName = searchIndexName;
        this.maxSplitsCount = maxSplitsCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(WritableConsts.COMPUTE_PARAMETERS);
        out.writeInt(maxSplitsCount);
        out.writeLong(splitSizeInMBs);
        out.writeUTF(computeMode);
        out.writeUTF(searchIndexName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.COMPUTE_PARAMETERS) {
            throw new IOException("broken input stream");
        }
        maxSplitsCount = in.readInt();
        splitSizeInMBs = in.readLong();
        computeMode = in.readUTF();
        searchIndexName = in.readUTF();
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

    public long getSplitSizeInMBs() {
        return splitSizeInMBs;
    }

    public String getComputeMode() {
        return computeMode;
    }

    public String getSearchIndexName() {
        return searchIndexName;
    }
}
