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

import java.util.List;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.Externalizable;

import org.apache.hadoop.io.Writable;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class BatchWriteWritable implements Writable, Externalizable {
    private List<RowChange> changes = new ArrayList<RowChange>();
    
    public BatchWriteWritable() {
    }

    public void addRowChange(RowChange row) {
        Preconditions.checkNotNull(row, "row must be nonnull.");
        this.changes.add(row);
    }

    public List<RowChange> getRowChanges() {
        return this.changes;
    }
    
    @Override public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.BATCH_WRITE);
        out.writeInt(this.changes.size());
        for(int i = 0; i < this.changes.size(); ++i) {
            RowChange rowChange = this.changes.get(i);
            if (rowChange instanceof RowPutChange) {
                out.writeByte(WritableConsts.PUT_ROW);
                new RowPutChangeWritable((RowPutChange) rowChange).write(out);
            } else if (rowChange instanceof RowDeleteChange) {
                out.writeByte(WritableConsts.DELETE_ROW);
                new RowDeleteChangeWritable((RowDeleteChange) rowChange).write(out);
            } else if (rowChange instanceof RowUpdateChange) {
                out.writeByte(WritableConsts.UPDATE_ROW);
                new RowUpdateChangeWritable((RowUpdateChange) rowChange).write(out);
            } else {
                throw new AssertionError(
                    "unsupported RowChange type: " + rowChange.getClass().getName());
            }
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        byte tagBatchWrite = in.readByte();
        if (tagBatchWrite != WritableConsts.BATCH_WRITE) {
            throw new IOException("broken input stream");
        }
        List<RowChange> changes = new ArrayList<RowChange>();
        int rowSz = in.readInt();
        for(int i = 0; i < rowSz; ++i) {
            byte tagRow = in.readByte();
            if (tagRow == WritableConsts.PUT_ROW) {
                changes.add(RowPutChangeWritable.read(in).getRowPutChange());
            } else if (tagRow == WritableConsts.DELETE_ROW) {
                changes.add(RowDeleteChangeWritable.read(in).getRowDeleteChange());
            } else if (tagRow == WritableConsts.UPDATE_ROW) {
                changes.add(RowUpdateChangeWritable.read(in).getRowUpdateChange());
            } else {
                throw new IOException("broken input stream");
            }
        }
        this.changes = changes;
    }

    public static BatchWriteWritable read(DataInput in) throws IOException {
        BatchWriteWritable w = new BatchWriteWritable();
        w.readFields(in);
        return w;
    }

    @Override public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
        this.readFields(in);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        this.write(out);
    }
}

