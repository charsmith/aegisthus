package com.netflix.aegisthus.io.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private ByteBuffer key;
    private ByteBuffer name;
    Comparator<ByteBuffer> comparator;

    public CompositeKey() {
    }

    public CompositeKey(ByteBuffer key, ByteBuffer name) {
        this.key = key;
        this.name = name;
    }

    @Override
    public void readFields(DataInput dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);

        this.key = ByteBuffer.wrap(bytes);

        length = dis.readInt();
        bytes = new byte[length];
        dis.readFully(bytes);

        this.name = ByteBuffer.wrap(bytes);
    }

    @Override
    public void write(DataOutput dos) throws IOException {
        dos.writeInt(key.array().length);
        dos.write(key.array());
        dos.writeInt(name.array().length);
        dos.write(name.array());
    }

    public ByteBuffer getKey() {
        return key;
    }

    @Override
    public int compareTo(CompositeKey other) {
        int compare = this.key.compareTo(other.key);
        if (compare == 0) {
            return comparator.compare(this.name, other.name);
        }
        return compare;
    }

    public void setComparator(Comparator<ByteBuffer> comparator) {
        this.comparator = comparator;
    }
}
