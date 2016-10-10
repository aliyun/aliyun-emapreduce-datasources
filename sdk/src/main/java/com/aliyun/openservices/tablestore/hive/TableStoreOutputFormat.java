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

