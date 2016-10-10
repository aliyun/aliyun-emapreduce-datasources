package com.aliyun.openservices.tablestore.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.EOFException;
import org.apache.hadoop.io.Writable;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RowWritable implements Writable {
    private Row row = null;

    public RowWritable() {
    }

    public RowWritable(Row row) {
        Preconditions.checkNotNull(row, "row should not be null.");
        this.row = row;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        Preconditions.checkNotNull(row, "row should not be null.");
        this.row = row;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkNotNull(row, "column should not be null.");
        out.writeByte(WritableConsts.ROW);
        new PrimaryKeyWritable(row.getPrimaryKey()).write(out);
        Column[] cols = row.getColumns();
        out.writeInt(cols.length);
        for(int i = 0; i < cols.length; ++i) {
            new ColumnWritable(cols[i]).write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte tagRow = in.readByte();
        if (tagRow != WritableConsts.ROW) {
            throw new IOException("broken input stream");
        }
        PrimaryKey pkey = PrimaryKeyWritable.read(in).getPrimaryKey();
        int sz = in.readInt();
        Column[] cols = new Column[sz];
        for(int i = 0; i < sz; ++i) {
            cols[i] = ColumnWritable.read(in).getColumn();
        }
        row = new Row(pkey, cols);
    }

    public static RowWritable read(DataInput in) throws IOException {
        RowWritable w = new RowWritable();
        w.readFields(in);
        return w;
    }
}

