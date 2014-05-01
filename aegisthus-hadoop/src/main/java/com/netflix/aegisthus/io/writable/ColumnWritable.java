package com.netflix.aegisthus.io.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.netflix.aegisthus.message.AegisthusProtos.Column;

public class ColumnWritable implements Writable {
    private Column column;

    public ColumnWritable() {
    }

    public ColumnWritable(Column column) {
        this.column = column;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        int length = arg0.readInt();
        byte[] bytes = new byte[length];
        arg0.readFully(bytes);
        column = Column.parseFrom(bytes);
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        byte[] bytes = column.toByteArray();
        arg0.write(bytes.length);
        arg0.write(bytes);
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

}
