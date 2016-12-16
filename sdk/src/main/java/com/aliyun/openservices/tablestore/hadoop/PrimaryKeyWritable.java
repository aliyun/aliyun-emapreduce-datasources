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
import java.io.EOFException;
import java.io.Externalizable;
import org.apache.hadoop.io.WritableComparable;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class PrimaryKeyWritable implements WritableComparable<PrimaryKeyWritable>, Externalizable {
    private PrimaryKey primaryKey = null;

    public PrimaryKeyWritable() {
    }

    public PrimaryKeyWritable(PrimaryKey pkey) {
        Preconditions.checkNotNull(pkey, "The primary key should not be null.");
        primaryKey = pkey;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey pkey) {
        Preconditions.checkNotNull(pkey, "The primary key should not be null.");
        this.primaryKey = pkey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(primaryKey, "primaryKey should not be null.");
        PrimaryKeyColumn[] cs = primaryKey.getPrimaryKeyColumns();
        out.writeByte(WritableConsts.PRIMARY_KEY);
        out.writeInt(cs.length);
        for(int i = 0; i < cs.length; ++i) {
            PrimaryKeyColumn c = cs[i];
            out.writeByte(WritableConsts.PRIMARY_KEY_COLUMN);
            out.writeUTF(c.getName());
            PrimaryKeyValue v = c.getValue();
            if (v.isInfMin()) {
                out.writeByte(WritableConsts.DATATYPE_INF_MIN);
            } else if (v.isInfMax()) {
                out.writeByte(WritableConsts.DATATYPE_INF_MAX);
            } else if (v.getType() == PrimaryKeyType.INTEGER) {
                out.writeByte(WritableConsts.DATATYPE_INT);
                out.writeLong(v.asLong());
            } else if (v.getType() == PrimaryKeyType.STRING) {
                out.writeByte(WritableConsts.DATATYPE_STR);
                out.writeUTF(v.asString());
            } else if (v.getType() == PrimaryKeyType.BINARY) {
                out.writeByte(WritableConsts.DATATYPE_BIN);
                byte[] bs = v.asBinary();
                out.writeInt(bs.length);
                out.write(bs);
            } else {
                throw new AssertionError("");
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte pkeyTag = in.readByte();
        if (pkeyTag != WritableConsts.PRIMARY_KEY) {
            throw new IOException("broken input stream");
        }
        int sz = in.readInt();
        PrimaryKeyColumn[] cs = new PrimaryKeyColumn[sz];
        for(int i = 0; i < sz; ++i) {
            byte tag = in.readByte();
            if (tag != WritableConsts.PRIMARY_KEY_COLUMN) {
                throw new IOException("broken input stream");
            }
            String name = in.readUTF();
            tag = in.readByte();
            if (tag == WritableConsts.DATATYPE_INF_MIN) {
                PrimaryKeyColumn c = new PrimaryKeyColumn(name, PrimaryKeyValue.INF_MIN);
                cs[i] = c;
            } else if (tag == WritableConsts.DATATYPE_INF_MAX) {
                PrimaryKeyColumn c = new PrimaryKeyColumn(name, PrimaryKeyValue.INF_MAX);
                cs[i] = c;
            } else if (tag == WritableConsts.DATATYPE_INT) {
                long v = in.readLong();
                PrimaryKeyColumn c = new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(v));
                cs[i] = c;
            } else if (tag == WritableConsts.DATATYPE_STR) {
                String v = in.readUTF();
                PrimaryKeyColumn c = new PrimaryKeyColumn(name, PrimaryKeyValue.fromString(v));
                cs[i] = c;
            } else if (tag == WritableConsts.DATATYPE_BIN) {
                int l = in.readInt();
                byte[] v = new byte[l];
                in.readFully(v);
                PrimaryKeyColumn c = new PrimaryKeyColumn(name, PrimaryKeyValue.fromBinary(v));
                cs[i] = c;
            } else {
                throw new IOException("broken input stream");
            }
        }
        primaryKey = new PrimaryKey(cs);
    }
       
    public static PrimaryKeyWritable read(DataInput in) throws IOException {
        PrimaryKeyWritable w = new PrimaryKeyWritable();
        w.readFields(in);
        return w;
    }

    @Override
    public int compareTo(PrimaryKeyWritable o) {
        Preconditions.checkNotNull(primaryKey, "The primary key should not be null.");
        Preconditions.checkNotNull(o.primaryKey, "The primary key should not be null.");
        return primaryKey.compareTo(o.primaryKey);
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
               ClassNotFoundException
    {
        this.readFields(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        this.write(out);
    }
}

