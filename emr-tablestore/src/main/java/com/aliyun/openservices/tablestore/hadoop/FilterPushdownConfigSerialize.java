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

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class FilterPushdownConfigSerialize implements Serializable, Writable {

    public boolean pushRangeLong;
    public boolean pushRangeString;

    public FilterPushdownConfigSerialize() {
    }

    public FilterPushdownConfigSerialize(boolean pushRangeLong, boolean pushRangeString) {
        this.pushRangeLong = pushRangeLong;
        this.pushRangeString = pushRangeString;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(WritableConsts.FILTER_PUSHDOWN_CONFIG);
        out.writeBoolean(pushRangeLong);
        out.writeBoolean(pushRangeString);
    }

    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.FILTER_PUSHDOWN_CONFIG) {
            throw new IOException("broken input stream");
        }
        pushRangeLong = in.readBoolean();
        pushRangeString = in.readBoolean();
    }


    public static FilterPushdownConfigSerialize read(DataInput in) throws IOException {
        FilterPushdownConfigSerialize w = new FilterPushdownConfigSerialize();
        w.readFields(in);
        return w;
    }

    public static FilterPushdownConfigSerialize deserialize(String in) {
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

    public String serialize() {
        return Utils.serialize(this);
    }

}
