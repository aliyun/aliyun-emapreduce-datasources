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

import java.util.Set;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.EOFException;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TimeRange;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RangeRowQueryCriteriaWritable implements Writable {
    private RangeRowQueryCriteria criteria = null;

    public RangeRowQueryCriteriaWritable() {
    }

    public RangeRowQueryCriteriaWritable(RangeRowQueryCriteria criteria) {
        Preconditions.checkNotNull(criteria, "criteria should not be null.");
        this.criteria = criteria;
    }

    public RangeRowQueryCriteria getRangeRowQueryCriteria() {
        return criteria;
    }

    @Override public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(criteria, "criteria should not be null.");
        Preconditions.checkNotNull(
            criteria.getTableName(),
            "criteria must have table name");
        out.writeByte(WritableConsts.GETRANGE_ROW_QUERY_CRITERIA);
        out.writeUTF(criteria.getTableName());

        if (criteria.numColumnsToGet() > 0) {
            Set<String> cols = criteria.getColumnsToGet();
            out.writeByte(WritableConsts.GETRANGE_COLUMNS_TO_GET);
            out.writeInt(cols.size());
            for(String c: cols) {
                out.writeUTF(c);
            }
        }
        if (criteria.hasSetMaxVersions()) {
            out.writeByte(WritableConsts.GETRANGE_MAX_VERSIONS);
            out.writeInt(criteria.getMaxVersions());
        }
        if (criteria.hasSetTimeRange()) {
            TimeRange tm = criteria.getTimeRange();
            out.writeByte(WritableConsts.GETRANGE_TIME_RANGE);
            out.writeLong(tm.getStart());
            out.writeLong(tm.getEnd());
        }
        if (criteria.hasSetCacheBlock()) {
            out.writeByte(WritableConsts.GETRANGE_CACHE_BLOCKS);
            out.writeBoolean(criteria.getCacheBlocks());
        }
        if (criteria.hasSetStartColumn()) {
            out.writeByte(WritableConsts.GETRANGE_START_COLUMN);
            out.writeUTF(criteria.getStartColumn());
        }
        if (criteria.hasSetEndColumn()) {
            out.writeByte(WritableConsts.GETRANGE_END_COLUMN);
            out.writeUTF(criteria.getEndColumn());
        }
        if (criteria.getLimit() != -1) {
            out.writeByte(WritableConsts.GETRANGE_LIMIT);
            out.writeInt(criteria.getLimit());
        }
        if (criteria.getDirection() != Direction.FORWARD) {
            Preconditions.checkArgument(
                criteria.getDirection() == Direction.BACKWARD,
                "direction must be either FORWARD or BACKWORD");
            out.writeByte(WritableConsts.GETRANGE_BACKWARDS);
        }
        if (criteria.getInclusiveStartPrimaryKey() != null) {
            out.writeByte(WritableConsts.GETRANGE_START_PKEY);
            new PrimaryKeyWritable(criteria.getInclusiveStartPrimaryKey()).write(out);
        }
        if (criteria.getExclusiveEndPrimaryKey() != null) {
            out.writeByte(WritableConsts.GETRANGE_END_PKEY);
            new PrimaryKeyWritable(criteria.getExclusiveEndPrimaryKey()).write(out);
        }
        if (criteria.hasSetFilter()) {
            new FilterWritable(criteria.getFilter()).write(out);
        }
    }

    @Override public void readFields(DataInput in) throws IOException {
        byte tagCriteria = in.readByte();
        if (tagCriteria != WritableConsts.GETRANGE_ROW_QUERY_CRITERIA) {
            throw new IOException("broken input stream");
        }
        String tblName = in.readUTF();
        criteria = new RangeRowQueryCriteria(tblName);

        while(true) {
            byte tag = nextTag(in);
            if (tag == WritableConsts.GETRANGE_COLUMNS_TO_GET) {
                int sz = in.readInt();
                for(int i = 0; i < sz; ++i) {
                    String c = in.readUTF();
                    criteria.addColumnsToGet(c);
                }
            } else if (tag == WritableConsts.GETRANGE_MAX_VERSIONS) {
                criteria.setMaxVersions(in.readInt());
            } else if (tag == WritableConsts.GETRANGE_TIME_RANGE) {
                long start = in.readLong();
                long end = in.readLong();
                criteria.setTimeRange(new TimeRange(start, end));
            } else if (tag == WritableConsts.GETRANGE_CACHE_BLOCKS) {
                criteria.setCacheBlocks(in.readBoolean());
            } else if (tag == WritableConsts.GETRANGE_START_COLUMN) {
                criteria.setStartColumn(in.readUTF());
            } else if (tag == WritableConsts.GETRANGE_END_COLUMN) {
                criteria.setEndColumn(in.readUTF());
            } else if (tag == WritableConsts.GETRANGE_LIMIT) {
                criteria.setLimit(in.readInt());
            } else if (tag == WritableConsts.GETRANGE_BACKWARDS) {
                criteria.setDirection(Direction.BACKWARD);
            } else if (tag == WritableConsts.GETRANGE_START_PKEY) {
                PrimaryKeyWritable pkey = PrimaryKeyWritable.read(in);
                criteria.setInclusiveStartPrimaryKey(pkey.getPrimaryKey());
            } else if (tag == WritableConsts.GETRANGE_END_PKEY) {
                PrimaryKeyWritable pkey = PrimaryKeyWritable.read(in);
                criteria.setExclusiveEndPrimaryKey(pkey.getPrimaryKey());
            } else if (tag == WritableConsts.FILTER) {
                criteria.setFilter(FilterWritable.readFilter(in));
            } else {
                break;
            }
        }
    }

    public static RangeRowQueryCriteriaWritable read(DataInput in) throws IOException {
        RangeRowQueryCriteriaWritable w = new RangeRowQueryCriteriaWritable();
        w.readFields(in);
        return w;
    }

    private byte nextTag(DataInput in) throws IOException {
        try {
            return in.readByte();
        } catch(EOFException ex) {
            return 0;
        }
    }
}

