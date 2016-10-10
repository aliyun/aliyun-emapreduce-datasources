package com.aliyun.openservices.tablestore.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.EOFException;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ColumnWritable implements Writable {
    private Column column = null;

    public ColumnWritable() {
    }

    public ColumnWritable(Column col) {
        Preconditions.checkNotNull(col, "The column should not be null.");
        column = col;
    }

    public Column getColumn() {
        return column;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(column, "column should not be null.");
        if (column.hasSetTimestamp()) {
            out.writeByte(WritableConsts.COLUMN_WITH_TIMESTAMP);
        } else {
            out.writeByte(WritableConsts.COLUMN_WITHOUT_TIMESTAMP);
        }
        out.writeUTF(column.getName());
        new ColumnValueWritable(column.getValue()).write(out);
        if (column.hasSetTimestamp()) {
            out.writeLong(column.getTimestamp());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tagColumn = in.readByte();
        if (tagColumn == WritableConsts.COLUMN_WITHOUT_TIMESTAMP) {
            String name = in.readUTF();
            ColumnValue val = ColumnValueWritable.read(in).getColumnValue();
            column = new Column(name, val);
        } else if (tagColumn == WritableConsts.COLUMN_WITH_TIMESTAMP) {
            String name = in.readUTF();
            ColumnValue val = ColumnValueWritable.read(in).getColumnValue();
            long ts = in.readLong();
            column = new Column(name, val, ts);
        } else {
            throw new IOException("broken input stream");
        }
    }

    public static ColumnWritable read(DataInput in) throws IOException {
        ColumnWritable w = new ColumnWritable();
        w.readFields(in);
        return w;
    }
}

