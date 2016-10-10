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
import java.io.EOFException;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.ColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.ColumnPaginationFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class FilterWritable implements Writable {
    private Filter filter;
    
    public FilterWritable() {
    }

    public FilterWritable(Filter filter) {
        Preconditions.checkNotNull(filter, "filter should not be null.");
        this.filter = filter;
    }

    public Filter getFilter() {
        return filter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(filter, "filter must be nonnull.");
        out.write(WritableConsts.FILTER);
        write(out, filter);
    }

    static private void write(DataOutput out, Filter filter) throws IOException {
        if (filter instanceof ColumnPaginationFilter) {
            ColumnPaginationFilter paging = (ColumnPaginationFilter) filter;
            out.write(WritableConsts.FILTER_COLUMN_PAGINATION);
            out.writeInt(paging.getOffset());
            out.writeInt(paging.getLimit());
        } else if (filter instanceof SingleColumnValueFilter) {
            SingleColumnValueFilter single = (SingleColumnValueFilter) filter;
            out.write(WritableConsts.FILTER_SINGLE_COLUMN);
            switch(single.getOperator()) {
            case EQUAL: {
                out.write(WritableConsts.FILTER_EQUAL);
                break;
            }
            case NOT_EQUAL: {
                out.write(WritableConsts.FILTER_NOT_EQUAL);
                break;
            }
            case GREATER_THAN: {
                out.write(WritableConsts.FILTER_GREATER);
                break;
            }
            case GREATER_EQUAL: {
                out.write(WritableConsts.FILTER_GREATER_EQUAL);
                break;
            }
            case LESS_THAN: {
                out.write(WritableConsts.FILTER_LESS);
                break;
            }
            case LESS_EQUAL: {
                out.write(WritableConsts.FILTER_LESS_EQUAL);
                break;
            }
            default: throw new AssertionError(
                "unknown operator: " + single.getOperator().toString());
            }
            out.writeUTF(single.getColumnName());
            out.writeBoolean(single.isPassIfMissing());
            out.writeBoolean(single.isLatestVersionsOnly());
            new ColumnValueWritable(single.getColumnValue()).write(out);
        } else if (filter instanceof CompositeColumnValueFilter) {
            CompositeColumnValueFilter comp = (CompositeColumnValueFilter) filter;
            out.write(WritableConsts.FILTER_COMPOSITED);
            switch(comp.getOperationType()) {
            case NOT: {
                out.write(WritableConsts.FILTER_NOT);
                break;
            }
            case AND: {
                out.write(WritableConsts.FILTER_AND);
                break;
            }
            case OR: {
                out.write(WritableConsts.FILTER_OR);
                break;
            }
            default: throw new AssertionError(
                "unknown logical operator: " + comp.getOperationType().toString());
            }
            out.writeInt(comp.getSubFilters().size());
            for(Filter f: comp.getSubFilters()) {
                write(out, f);
            }
        } else {
            throw new AssertionError("unknown filter type " + filter.getFilterType().toString());
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.FILTER) {
            throw new IOException("broken input stream");
        }
        filter = readFilter(in);
    }

    static Filter readFilter(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag == WritableConsts.FILTER_COLUMN_PAGINATION) {
            int offset = in.readInt();
            int limit = in.readInt();
            return new ColumnPaginationFilter(limit, offset);
        } else {
            return readColumnValueFilter(in, tag);
        }
    }

    private static ColumnValueFilter readColumnValueFilter(DataInput in, byte tag)
        throws IOException
    {
        if (tag == WritableConsts.FILTER_SINGLE_COLUMN) {
            byte opTag = in.readByte();
            SingleColumnValueFilter.CompareOperator op;
            if (opTag == WritableConsts.FILTER_EQUAL) {
                op = SingleColumnValueFilter.CompareOperator.EQUAL;
            } else if (opTag == WritableConsts.FILTER_NOT_EQUAL) {
                op = SingleColumnValueFilter.CompareOperator.NOT_EQUAL;
            } else if (opTag == WritableConsts.FILTER_GREATER) {
                op = SingleColumnValueFilter.CompareOperator.GREATER_THAN;
            } else if (opTag == WritableConsts.FILTER_GREATER_EQUAL) {
                op = SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
            } else if (opTag == WritableConsts.FILTER_LESS) {
                op = SingleColumnValueFilter.CompareOperator.LESS_THAN;
            } else if (opTag == WritableConsts.FILTER_LESS_EQUAL) {
                op = SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
            } else {
                throw new IOException("broken input stream");
            }
            String name = in.readUTF();
            boolean passIfMissing = in.readBoolean();
            boolean latestVersionOnly = in.readBoolean();
            ColumnValue value = ColumnValueWritable.read(in).getColumnValue();
            return new SingleColumnValueFilter(name, op, value)
                .setPassIfMissing(passIfMissing)
                .setLatestVersionsOnly(latestVersionOnly);
        } else if (tag == WritableConsts.FILTER_COMPOSITED) {
            byte opTag = in.readByte();
            CompositeColumnValueFilter filter;
            if (opTag == WritableConsts.FILTER_NOT) {
                filter = new CompositeColumnValueFilter(
                    CompositeColumnValueFilter.LogicOperator.NOT);
            } else if (opTag == WritableConsts.FILTER_AND) {
                filter = new CompositeColumnValueFilter(
                    CompositeColumnValueFilter.LogicOperator.AND);
            } else if (opTag == WritableConsts.FILTER_OR) {
                filter = new CompositeColumnValueFilter(
                    CompositeColumnValueFilter.LogicOperator.OR);
            } else {
                throw new IOException("broken input stream");
            }
            int sz = in.readInt();
            for(int i = 0; i < sz; ++i) {
                byte nxtTag = in.readByte();
                ColumnValueFilter f = readColumnValueFilter(in, nxtTag);
                filter.addFilter(f);
            }
            return filter;
        } else {
            throw new IOException("broken input stream");
        }
    }
    
    public static FilterWritable read(DataInput in) throws IOException {
        FilterWritable w = new FilterWritable();
        w.readFields(in);
        return w;
    }
}

