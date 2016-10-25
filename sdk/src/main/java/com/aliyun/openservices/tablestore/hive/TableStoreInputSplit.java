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

package com.aliyun.openservices.tablestore.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TableStoreInputSplit implements InputSplit, Writable {
    private com.aliyun.openservices.tablestore.hadoop.TableStoreInputSplit delegated;

    public TableStoreInputSplit() {
    }

    public TableStoreInputSplit(
        com.aliyun.openservices.tablestore.hadoop.TableStoreInputSplit delegated)
    {
        this.delegated = delegated;
    }

    public com.aliyun.openservices.tablestore.hadoop.TableStoreInputSplit getDelegated() {
        return this.delegated;
    }

    @Override
    public long getLength() throws IOException {
        Preconditions.checkNotNull(this.delegated, "delegated should not be null.");
        try {
            return this.delegated.getLength();
        } catch (InterruptedException ex) {
            // intend to do nothing
            return 0;
        }
    }

    @Override
    public String[] getLocations() throws IOException {
        Preconditions.checkNotNull(this.delegated, "delegated should not be null.");
        try {
            return this.delegated.getLocations();
        } catch(InterruptedException ex) {
            // intend to do nothing
            return null;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(this.delegated, "delegated should not be null.");
        this.delegated.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.delegated = com.aliyun.openservices.tablestore.hadoop.TableStoreInputSplit
            .read(in);
    }
}

