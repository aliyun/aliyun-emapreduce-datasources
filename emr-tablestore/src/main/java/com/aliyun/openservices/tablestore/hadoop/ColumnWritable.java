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
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ColumnWritable implements Writable {
    private Column column = null;

    public ColumnWritable() {
    }

    public ColumnWritable(Column col) {
        Preconditions.checkNotNull(col, "The column should not be null.");
        column = col;
    }

    public Column getColumn() {
        return column;
    }

    @Override public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(column, "column should not be null.");
        if (column.hasSetTimestamp()) {
            out.writeByte(WritableConsts.COLUMN_WITH_TIMESTAMP);
        } else {
            out.writeByte(WritableConsts.COLUMN_WITHOUT_TIMESTAMP);
        }
        out.writeUTF(column.getName());
        new ColumnValueWritable(column.getValue()).write(out);
        if (column.hasSetTimestamp()) {
            out.writeLong(column.getTimestamp());
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        byte tagColumn = in.readByte();
        if (tagColumn == WritableConsts.COLUMN_WITHOUT_TIMESTAMP) {
            String name = in.readUTF();
            ColumnValue val = ColumnValueWritable.read(in).getColumnValue();
            column = new Column(name, val);
        } else if (tagColumn == WritableConsts.COLUMN_WITH_TIMESTAMP) {
            String name = in.readUTF();
            ColumnValue val = ColumnValueWritable.read(in).getColumnValue();
            long ts = in.readLong();
            column = new Column(name, val, ts);
        } else {
            throw new IOException("broken input stream");
        }
    }

    public static ColumnWritable read(DataInput in) throws IOException {
        ColumnWritable w = new ColumnWritable();
        w.readFields(in);
        return w;
    }
}

