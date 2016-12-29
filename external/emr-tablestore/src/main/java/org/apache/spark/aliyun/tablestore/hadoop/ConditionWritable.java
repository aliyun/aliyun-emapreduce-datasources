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

import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.RowExistenceExpectation;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.CompositeColumnValueCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.apache.spark.aliyun.tablestore.hadoop.FilterWritable;
import org.apache.spark.aliyun.tablestore.hadoop.WritableConsts;

public class ConditionWritable implements Writable, Externalizable {
    private Condition condition;

    public ConditionWritable() {
    }

    public ConditionWritable(Condition condition) {
        Preconditions.checkNotNull(condition, "condition must be nonnull.");
        this.condition = condition;
    }

    public Condition getCondition() {
        return condition;
    }

    @Override public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.CONDITION);
        switch(condition.getRowExistenceExpectation()) {
        case IGNORE: {
            out.writeByte(WritableConsts.ROW_CONDITION_IGNORE);
            break;
        }
        case EXPECT_EXIST: {
            out.writeByte(WritableConsts.ROW_CONDITION_EXPECT_EXIST);
            break;
        }
        case EXPECT_NOT_EXIST: {
            out.writeByte(WritableConsts.ROW_CONDITION_EXPECT_NOT_EXIST);
            break;
        }
        }
        ColumnCondition cc = condition.getColumnCondition();
        if (cc == null) {
            out.writeByte(WritableConsts.COLUMN_CONDITION_NONE);
        } else {
            out.writeByte(WritableConsts.COLUMN_CONDITION_PRESENT);
            switch(cc.getConditionType()) {
            case COMPOSITE_COLUMN_VALUE_CONDITION: {
                new FilterWritable(((CompositeColumnValueCondition) cc).toFilter()).write(out);
                break;
            }
            case SINGLE_COLUMN_VALUE_CONDITION: {
                new FilterWritable(((SingleColumnValueCondition) cc).toFilter()).write(out);
                break;
            }
            }
        }

    }

    @Override public void readFields(DataInput in) throws IOException {
        byte tagCondition = in.readByte();
        if (tagCondition != WritableConsts.CONDITION) {
            throw new IOException("broken input stream");
        }

        Condition condition = new Condition();

        byte tagRowCondition = in.readByte();
        if (tagRowCondition == WritableConsts.ROW_CONDITION_IGNORE) {
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
        } else if (tagRowCondition == WritableConsts.ROW_CONDITION_EXPECT_EXIST) {
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
        } else if (tagRowCondition == WritableConsts.ROW_CONDITION_EXPECT_NOT_EXIST) {
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
        } else {
            throw new IOException("broken input stream");
        }

        byte tagColCondition = in.readByte();
        if (tagColCondition == WritableConsts.COLUMN_CONDITION_NONE) {
            // intend to do nothing
        } else {
            Filter filter = FilterWritable.read(in).getFilter();
            condition.setColumnCondition(filterToColumnCondition(filter));
        }

        this.condition = condition;
    }

    public static ConditionWritable read(DataInput in) throws IOException {
        ConditionWritable w = new ConditionWritable();
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

    private ColumnCondition filterToColumnCondition(Filter filter) {
        if (filter instanceof SingleColumnValueFilter) {
            return ((SingleColumnValueFilter) filter).toCondition();
        } else if (filter instanceof CompositeColumnValueFilter) {
            return ((CompositeColumnValueFilter) filter).toCondition();
        } else {
            throw new AssertionError("unknown filter type");
        }
    }
}

