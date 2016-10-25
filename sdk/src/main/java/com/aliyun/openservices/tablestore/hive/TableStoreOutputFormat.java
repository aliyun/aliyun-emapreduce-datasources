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

import java.io.IOException;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import com.aliyun.openservices.tablestore.hadoop.PrimaryKeyWritable;
import com.aliyun.openservices.tablestore.hadoop.RowWritable;

public class TableStoreOutputFormat implements OutputFormat<PrimaryKeyWritable, RowWritable> {
    @Override
    public RecordWriter<PrimaryKeyWritable, RowWritable> getRecordWriter(
        FileSystem ignored,
        JobConf job,
        String name,
        Progressable progress)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkOutputSpecs(
        FileSystem ignored,
        JobConf job)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }
}

