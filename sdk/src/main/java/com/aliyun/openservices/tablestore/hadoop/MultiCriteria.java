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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class MultiCriteria implements Writable {
    private List<RangeRowQueryCriteria> criteria = new ArrayList<RangeRowQueryCriteria>();

    public MultiCriteria() {
    }

    public List<RangeRowQueryCriteria> getCriteria() {
        return Collections.unmodifiableList(criteria);
    }

    public void addCriteria(RangeRowQueryCriteria criteria) {
        this.criteria.add(criteria);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MultiCriteria)) {
            return false;
        }
        MultiCriteria a = (MultiCriteria) obj;
        return this.criteria.equals(a.criteria);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.MULTI_CRITERIA);
        out.writeInt(criteria.size());
        for(RangeRowQueryCriteria c: criteria) {
            new RangeRowQueryCriteriaWritable(c).write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.MULTI_CRITERIA) {
            throw new IOException("broken input stream");
        }
        List<RangeRowQueryCriteria> newCriteria = new ArrayList<RangeRowQueryCriteria>();
        int sz = in.readInt();
        for(int i = 0; i < sz; ++i) {
            newCriteria.add(
                RangeRowQueryCriteriaWritable.read(in).getRangeRowQueryCriteria());
        }
        criteria = newCriteria;
    }

    public static MultiCriteria read(DataInput in) throws IOException {
        MultiCriteria w = new MultiCriteria();
        w.readFields(in);
        return w;
    }

    public String serialize() {
        return Utils.serialize(this);
    }

    public static MultiCriteria deserialize(String in) {
        byte[] buf = Base64.decodeBase64(in);
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        DataInputStream din = new DataInputStream(is);
        try {
            return read(din);
        } catch(IOException ex) {
            return null;
        }
    }
}

