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

import com.alicloud.openservices.tablestore.ecosystem.Filter;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class TableStoreFilterWritable implements Writable {
    private Filter filter;
    private List<String> requiredColumns;

    public TableStoreFilterWritable() {

    }

    public TableStoreFilterWritable(Filter filter, List<String> requiredColumns) {
        this.filter = filter;
        this.requiredColumns = requiredColumns;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(WritableConsts.FILTER);
        write(out, filter);
        write(out, requiredColumns);
    }

    public void write(DataOutput out, Filter filter) throws IOException {
        if (filter.isNested()) {
            out.write(WritableConsts.FILTER_COMPOSITED);
            switch (filter.getLogicOperator()) {
                case OR:
                    out.write(WritableConsts.FILTER_OR);
                    break;
                case AND:
                    out.write(WritableConsts.FILTER_AND);
                    break;
                case NOT:
                    out.write(WritableConsts.FILTER_NOT);
                    break;
                default:
                    throw new AssertionError(
                            "unknown logical operator: " + filter.getLogicOperator().toString());
            }
            out.writeInt(filter.getSubFilters().size());
            for (Filter f : filter.getSubFilters()) {
                write(out, f);
            }
        } else if (filter.getCompareOperator().equals(Filter.CompareOperator.EMPTY_FILTER)) {
            // empty filter
            out.write(WritableConsts.FILTER_EMPTY);
        } else {
            out.write(WritableConsts.FILTER_SINGLE_COLUMN);
            switch (filter.getCompareOperator()) {
                case EQUAL:
                    out.write(WritableConsts.FILTER_EQUAL);
                    break;
                case NOT_EQUAL:
                    out.write(WritableConsts.FILTER_NOT_EQUAL);
                    break;
                case GREATER_THAN:
                    out.write(WritableConsts.FILTER_GREATER);
                    break;
                case GREATER_EQUAL:
                    out.write(WritableConsts.FILTER_GREATER_EQUAL);
                    break;
                case LESS_THAN:
                    out.write(WritableConsts.FILTER_LESS);
                    break;
                case LESS_EQUAL:
                    out.write(WritableConsts.FILTER_LESS_EQUAL);
                    break;
                case START_WITH:
                    out.write(WritableConsts.FILTER_START_WITH);
                    break;
                case IN:
                    out.write(WritableConsts.FILTER_IN);
                    break;
                case IS_NULL:
                    out.write(WritableConsts.FILTER_IS_NULL);
                    break;
                default:
                    throw new AssertionError(
                            "unknown operator: " + filter.getCompareOperator().toString());
            }
            out.writeUTF(filter.getColumnName());
            if (filter.getColumnValue() != null) {
                new ColumnValueWritable(filter.getColumnValue()).write(out);
            }
            if (filter.getColumnValuesForInOperator() != null) {
                out.writeInt(filter.getColumnValuesForInOperator().size());
                for (ColumnValue columnValue : filter.getColumnValuesForInOperator()) {
                    new ColumnValueWritable(columnValue).write(out);
                }
            }
        }
    }

    public void write(DataOutput out, List<String> requiredColumns) throws IOException {
        out.write(WritableConsts.REQUIRED_COLUMNS);
        out.writeInt(requiredColumns.size());
        for (String column : requiredColumns) {
            out.writeUTF(column);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.FILTER) {
            throw new IOException("broken input stream");
        }
        filter = readFilter(in);
        requiredColumns = readRequiredColumns(in);
    }

    public static TableStoreFilterWritable read(DataInput in) throws IOException {
        TableStoreFilterWritable f = new TableStoreFilterWritable();
        f.readFields(in);
        return f;
    }

    private static Filter readFilter(DataInput in) throws IOException {
        byte tag = in.readByte();
        Filter.CompareOperator co;
        if (tag == WritableConsts.FILTER_SINGLE_COLUMN) {
            byte opTag = in.readByte();
            if (opTag == WritableConsts.FILTER_EQUAL) {
                co = Filter.CompareOperator.EQUAL;
            } else if (opTag == WritableConsts.FILTER_NOT_EQUAL) {
                co = Filter.CompareOperator.NOT_EQUAL;
            } else if (opTag == WritableConsts.FILTER_GREATER) {
                co = Filter.CompareOperator.GREATER_THAN;
            } else if (opTag == WritableConsts.FILTER_GREATER_EQUAL) {
                co = Filter.CompareOperator.GREATER_EQUAL;
            } else if (opTag == WritableConsts.FILTER_LESS) {
                co = Filter.CompareOperator.LESS_THAN;
            } else if (opTag == WritableConsts.FILTER_LESS_EQUAL) {
                co = Filter.CompareOperator.LESS_EQUAL;
            } else if (opTag == WritableConsts.FILTER_START_WITH) {
                co = Filter.CompareOperator.START_WITH;
            } else if (opTag == WritableConsts.FILTER_IN) {
                co = Filter.CompareOperator.IN;
            } else if (opTag == WritableConsts.FILTER_IS_NULL) {
                co = Filter.CompareOperator.IS_NULL;
            } else {
                throw new IOException("broken input stream");
            }
            String name = in.readUTF();
            if (opTag == WritableConsts.FILTER_IS_NULL) {
                return new Filter(co, name);
            } else if (opTag == WritableConsts.FILTER_IN) {
                int size = in.readInt();
                List<ColumnValue> values = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    ColumnValue value = ColumnValueWritable.read(in).getColumnValue();
                    values.add(value);
                }
                return new Filter(co, name, values);
            } else {
                ColumnValue value = ColumnValueWritable.read(in).getColumnValue();
                return new Filter(co, name, value);
            }
        } else if (tag == WritableConsts.FILTER_COMPOSITED) {
            byte opTag = in.readByte();
            Filter.LogicOperator lo;
            if (opTag == WritableConsts.FILTER_AND) {
                lo = Filter.LogicOperator.AND;
            } else if (opTag == WritableConsts.FILTER_OR) {
                lo = Filter.LogicOperator.OR;
            } else if (opTag == WritableConsts.FILTER_NOT) {
                lo = Filter.LogicOperator.NOT;
            } else {
                throw new IOException("broken input stream");
            }
            int sz = in.readInt();
            List<Filter> subFilters = new ArrayList<Filter>(sz);
            for (int i = 0; i < sz; i++) {
                subFilters.add(readFilter(in));
            }
            return new Filter(lo, subFilters);
        } else if (tag == WritableConsts.FILTER_EMPTY) {
            return Filter.emptyFilter();
        } else {
            throw new IOException("broken input stream");
        }
    }

    private static List<String> readRequiredColumns(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.REQUIRED_COLUMNS) {
            throw new IOException("broken input stream");
        }
        int sz = in.readInt();
        List<String> requiredColumns = new ArrayList<String>(sz);
        for (int i = 0; i < sz; i++) {
            requiredColumns.add(in.readUTF());
        }
        return requiredColumns;
    }

    public String serialize() {
        return Utils.serialize(this);
    }

    public static TableStoreFilterWritable deserialize(String in) {
        if (in == null) {
            return null;
        }
        byte[] buf = Base64.decodeBase64(in);
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(is);
        try {
            return read(din);
        } catch (IOException ex) {
            return null;
        }
    }

    public Filter getFilter() {
        return filter;
    }

    public List<String> getRequiredColumns() {
        return requiredColumns;
    }
}
