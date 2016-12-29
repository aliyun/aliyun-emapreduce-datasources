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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.Externalizable;

import org.apache.hadoop.io.Writable;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RowDeleteChangeWritable implements Writable, Externalizable {
    private RowDeleteChange delRow;

    public RowDeleteChangeWritable() {
    }

    public RowDeleteChangeWritable(RowDeleteChange delRow) {
        Preconditions.checkNotNull(delRow, "delRow must be nonnull.");
        this.delRow = delRow;
    }

    public RowDeleteChange getRowDeleteChange() {
        return delRow;
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeUTF(delRow.getTableName());
        new PrimaryKeyWritable(delRow.getPrimaryKey()).write(out);
        new ConditionWritable(delRow.getCondition()).write(out);
    }

    @Override public void readFields(DataInput in) throws IOException {
        String tableName = in.readUTF();
        PrimaryKey pkey = PrimaryKeyWritable.read(in).getPrimaryKey();
        RowDeleteChange delRow = new RowDeleteChange(tableName, pkey);
        delRow.setCondition(ConditionWritable.read(in).getCondition());
        this.delRow = delRow;
    }

    public static RowDeleteChangeWritable read(DataInput in) throws IOException {
        RowDeleteChangeWritable w = new RowDeleteChangeWritable();
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
