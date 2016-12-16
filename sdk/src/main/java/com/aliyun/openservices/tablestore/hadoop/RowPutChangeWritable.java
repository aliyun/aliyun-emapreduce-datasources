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
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RowPutChangeWritable implements Writable, Externalizable {
    private RowPutChange putRow;

    public RowPutChangeWritable() {
    }

    public RowPutChangeWritable(RowPutChange putRow) {
        Preconditions.checkNotNull(putRow, "putRow must be nonnull.");
        this.putRow = putRow;
    }

    public RowPutChange getRowPutChange() {
        return this.putRow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(putRow.getTableName());
        new PrimaryKeyWritable(putRow.getPrimaryKey()).write(out);
        new ConditionWritable(putRow.getCondition()).write(out);
        List<Column> cols = putRow.getColumnsToPut();
        out.writeInt(cols.size());
        for(Column col: cols) {
            new ColumnWritable(col).write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String tableName = in.readUTF();
        PrimaryKey pkey = PrimaryKeyWritable.read(in).getPrimaryKey();

        RowPutChange putRow = new RowPutChange(tableName, pkey);
        putRow.setCondition(ConditionWritable.read(in).getCondition());
        int attrSz = in.readInt();
        for(int j = 0; j < attrSz; ++j) {
            putRow.addColumn(ColumnWritable.read(in).getColumn());
        }

        this.putRow = putRow;
    }

    public static RowPutChangeWritable read(DataInput in) throws IOException {
        RowPutChangeWritable w = new RowPutChangeWritable();
        w.readFields(in);
        return w;
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        this.readFields(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        this.write(out);
    }
}
