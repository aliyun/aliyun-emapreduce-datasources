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

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Split;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComputeSplitWritable implements Writable {
    private Split split;

    public ComputeSplitWritable() {
    }

    public ComputeSplitWritable(Split split) {
        this.split = split;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(WritableConsts.COMPUTE_SPLIT);
        write(out, split);
    }

    public void write(DataOutput out, Split split) throws IOException {
        out.writeUTF(split.getLocation());
        new PrimaryKeyWritable(split.getLowerBound()).write(out);
        new PrimaryKeyWritable(split.getUpperBound()).write(out);
    }

    public static ComputeSplitWritable read(DataInput in) throws IOException {
        ComputeSplitWritable cw = new ComputeSplitWritable();
        cw.readFields(in);
        return cw;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.COMPUTE_SPLIT) {
            throw new IOException("broken input stream");
        }
        split = readSplit(in);
    }

    private static Split readSplit(DataInput in) throws IOException {
        String location = in.readUTF();
        PrimaryKey lower = PrimaryKeyWritable.read(in).getPrimaryKey();
        PrimaryKey upper = PrimaryKeyWritable.read(in).getPrimaryKey();
        return new Split(location, lower, upper);
    }

    public Split getSplit() {
        return split;
    }
}
