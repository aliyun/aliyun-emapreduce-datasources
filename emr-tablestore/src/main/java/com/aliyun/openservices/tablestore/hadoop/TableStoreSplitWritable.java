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
import com.alicloud.openservices.tablestore.ecosystem.TablestoreSplit;
import com.alicloud.openservices.tablestore.model.Split;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableStoreSplitWritable implements Writable {
    private TablestoreSplit split;

    public TableStoreSplitWritable() {
    }

    public TableStoreSplitWritable(TablestoreSplit split) {
        this.split = split;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(WritableConsts.SPLIT);
        out.writeUTF(split.getType().name());
        out.writeUTF(split.getSplitName());
        out.writeUTF(split.getTableName());
        new ComputeSplitWritable(split.getKvSplit()).write(out);
        new TableStoreFilterWritable(split.getFilter(), split.getRequiredColumns()).write(out);
    }

    public static TableStoreSplitWritable read(DataInput in) throws IOException {
        TableStoreSplitWritable sw = new TableStoreSplitWritable();
        sw.readFields(in);
        return sw;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tag = in.readByte();
        if (tag != WritableConsts.SPLIT) {
            throw new IOException("broken input stream");
        }
        split = readSplit(in);
    }

    static TablestoreSplit readSplit(DataInput in) throws IOException {
        TablestoreSplit.SplitType splitType = TablestoreSplit.SplitType.valueOf(in.readUTF());
        String splitName = in.readUTF();
        String tableName = in.readUTF();
        Split kvSplit = ComputeSplitWritable.read(in).getSplit();
        TableStoreFilterWritable fw = TableStoreFilterWritable.read(in);
        TablestoreSplit rtSplit = new TablestoreSplit(splitType, fw.getFilter(), fw.getRequiredColumns());
        rtSplit.setSplitName(splitName);
        rtSplit.setTableName(tableName);
        rtSplit.setKvSplit(kvSplit);
        return rtSplit;
    }

    public TablestoreSplit getSplit() {
        return split;
    }

    public void setSplit(TablestoreSplit split) {
        this.split = split;
    }
}
