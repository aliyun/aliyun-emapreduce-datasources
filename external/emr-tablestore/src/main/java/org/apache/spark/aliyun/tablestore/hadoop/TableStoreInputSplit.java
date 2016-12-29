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
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreInputSplit extends InputSplit implements Writable {
    private RangeRowQueryCriteria criteria = null;

    public TableStoreInputSplit() {
    }

    public TableStoreInputSplit(RangeRowQueryCriteria criteria) {
        Preconditions.checkNotNull(criteria, "criteria should not be null.");
        this.criteria = criteria;
    }

    public RangeRowQueryCriteria getRangeRowQueryCriteria() {
        return criteria;
    }

    /**
     * Returns the length of the split.
     *
     * @return The length of the split.
     * @see org.apache.hadoop.mapreduce.InputSplit#getLength()
     */
    @Override public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {};
    }

    @Override public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(criteria, "criteria should not be null.");
        new RangeRowQueryCriteriaWritable(criteria).write(out);
    }

    @Override public void readFields(DataInput in) throws IOException {
        criteria = RangeRowQueryCriteriaWritable.read(in).getRangeRowQueryCriteria();
    }

    public static TableStoreInputSplit read(DataInput in) throws IOException {
        TableStoreInputSplit w = new TableStoreInputSplit();
        w.readFields(in);
        return w;
    }
}

