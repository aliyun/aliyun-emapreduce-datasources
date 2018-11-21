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

import java.io.DataInput;
import java.io.ObjectInput;
import java.io.DataOutput;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.Externalizable;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RowWritable implements Writable, Externalizable {
    private Row row = null;

    public RowWritable() {
    }

    public RowWritable(Row row) {
        Preconditions.checkNotNull(row, "row should not be null.");
        this.row = row;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        Preconditions.checkNotNull(row, "row should not be null.");
        this.row = row;
    }

    @Override public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(row, "column should not be null.");
        out.writeByte(WritableConsts.ROW);
        new PrimaryKeyWritable(row.getPrimaryKey()).write(out);
        Column[] cols = row.getColumns();
        out.writeInt(cols.length);
        for(int i = 0; i < cols.length; ++i) {
            new ColumnWritable(cols[i]).write(out);
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        byte tagRow = in.readByte();
        if (tagRow != WritableConsts.ROW) {
            throw new IOException("broken input stream");
        }
        PrimaryKey pkey = PrimaryKeyWritable.read(in).getPrimaryKey();
        int sz = in.readInt();
        Column[] cols = new Column[sz];
        for(int i = 0; i < sz; ++i) {
            cols[i] = ColumnWritable.read(in).getColumn();
        }
        row = new Row(pkey, cols);
    }

    public static RowWritable read(DataInput in) throws IOException {
        RowWritable w = new RowWritable();
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

