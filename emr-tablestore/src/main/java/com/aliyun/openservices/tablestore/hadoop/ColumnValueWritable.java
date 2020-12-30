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
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ColumnValueWritable implements Writable {
    private ColumnValue columnValue = null;

    public ColumnValueWritable() {
    }

    public ColumnValueWritable(ColumnValue colVal) {
        Preconditions.checkNotNull(colVal, "The column value should not be null.");
        columnValue = colVal;
    }

    public ColumnValue getColumnValue() {
        return columnValue;
    }

    @Override public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(columnValue, "columnValue should not be null.");
        out.writeByte(WritableConsts.COLUMN_VALUE);
        switch(columnValue.getType()) {
        case INTEGER: {
            out.writeByte(WritableConsts.DATATYPE_INT);
            out.writeLong(columnValue.asLong());
            break;
        }
        case STRING: {
            out.writeByte(WritableConsts.DATATYPE_STR);
            out.writeUTF(columnValue.asString());
            break;
        }
        case BINARY: {
            out.writeByte(WritableConsts.DATATYPE_BIN);
            byte[] v = columnValue.asBinary();
            out.writeInt(v.length);
            out.write(v);
            break;
        }
        case DOUBLE: {
            out.writeByte(WritableConsts.DATATYPE_DBL);
            out.writeDouble(columnValue.asDouble());
            break;
        }
        case BOOLEAN: {
            out.writeByte(WritableConsts.DATATYPE_BOOL);
            out.writeBoolean(columnValue.asBoolean());
            break;
        }
        default: {
            throw new IOException("unknown ColumnType");
        }
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        byte columnValueTag = in.readByte();
        if (columnValueTag != WritableConsts.COLUMN_VALUE) {
            throw new IOException("broken input stream");
        }
        byte type = in.readByte();
        if (type == WritableConsts.DATATYPE_INT) {
            long v = in.readLong();
            columnValue = ColumnValue.fromLong(v);
        } else if (type == WritableConsts.DATATYPE_STR) {
            String v = in.readUTF();
            columnValue = ColumnValue.fromString(v);
        } else if (type == WritableConsts.DATATYPE_BIN) {
            int len = in.readInt();
            byte[] v = new byte[len];
            in.readFully(v);
            columnValue = ColumnValue.fromBinary(v);
        } else if (type == WritableConsts.DATATYPE_DBL) {
            double v = in.readDouble();
            columnValue = ColumnValue.fromDouble(v);
        } else if (type == WritableConsts.DATATYPE_BOOL) {
            boolean v = in.readBoolean();
            columnValue = ColumnValue.fromBoolean(v);
        } else {
            throw new IOException("broken input stream");
        }
    }

    public static ColumnValueWritable read(DataInput in) throws IOException {
        ColumnValueWritable w = new ColumnValueWritable();
        w.readFields(in);
        return w;
    }
}

