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

package org.apache.spark.aliyun.tablestore.hadoop;

import java.util.List;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.Externalizable;

import org.apache.hadoop.io.Writable;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Pair;

public class RowUpdateChangeWritable implements Writable, Externalizable {
    private RowUpdateChange updateRow;

    public RowUpdateChangeWritable() {
    }

    public RowUpdateChangeWritable(RowUpdateChange updateRow) {
        Preconditions.checkNotNull(updateRow, "updateRow must be nonnull.");
        this.updateRow = updateRow;
    }

    public RowUpdateChange getRowUpdateChange() {
        return this.updateRow;
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeUTF(updateRow.getTableName());
        new PrimaryKeyWritable(updateRow.getPrimaryKey()).write(out);
        new ConditionWritable(updateRow.getCondition()).write(out);
        List<Pair<Column, RowUpdateChange.Type>> cols = updateRow.getColumnsToUpdate();
        out.writeInt(cols.size());
        for(Pair<Column, RowUpdateChange.Type> pair: cols) {
            Column col = pair.getFirst();
            switch(pair.getSecond()) {
            case PUT: {
                out.writeByte(WritableConsts.UPDATE_ROW_DATA);
                new ColumnWritable(col).write(out);
                break;
            }
            case DELETE: {
                out.writeByte(WritableConsts.UPDATE_ROW_DELETE_ONE_VERSION);
                out.writeUTF(col.getName());
                out.writeLong(col.getTimestamp());
                break;
            }
            case DELETE_ALL: {
                out.writeByte(WritableConsts.UPDATE_ROW_DELETE_ALL_VERSIONS);
                out.writeUTF(col.getName());
                break;
            }
            }
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        String tableName = in.readUTF();
        PrimaryKey pkey = PrimaryKeyWritable.read(in).getPrimaryKey();
        
        RowUpdateChange updateRow = new RowUpdateChange(tableName, pkey);
        updateRow.setCondition(ConditionWritable.read(in).getCondition());
        int sz = in.readInt();
        for(int i = 0; i < sz; ++i) {
            byte tagCol = in.readByte();
            if (tagCol == WritableConsts.UPDATE_ROW_DATA) {
                updateRow.put(ColumnWritable.read(in).getColumn());
            } else if (tagCol == WritableConsts.UPDATE_ROW_DELETE_ONE_VERSION) {
                String colName = in.readUTF();
                long ts = in.readLong();
                updateRow.deleteColumn(colName, ts);
            } else if (tagCol == WritableConsts.UPDATE_ROW_DELETE_ALL_VERSIONS) {
                String colName = in.readUTF();
                updateRow.deleteColumns(colName);
            } else {
                throw new IOException("broken input stream");
            }
        }

        this.updateRow = updateRow;
    }

    public static RowUpdateChangeWritable read(DataInput in) throws IOException {
        RowUpdateChangeWritable w = new RowUpdateChangeWritable();
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
