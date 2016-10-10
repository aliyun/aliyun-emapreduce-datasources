package com.aliyun.openservices.tablestore.hadoop;

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
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(criteria, "criteria should not be null.");
        new RangeRowQueryCriteriaWritable(criteria).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        criteria = RangeRowQueryCriteriaWritable.read(in).getRangeRowQueryCriteria();
    }

    public static TableStoreInputSplit read(DataInput in) throws IOException {
        TableStoreInputSplit w = new TableStoreInputSplit();
        w.readFields(in);
        return w;
    }
}

