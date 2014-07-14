package com.netflix.aegisthus.io.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.IColumnSerializer.Flag;
import org.apache.cassandra.io.sstable.Descriptor.Version;
import org.apache.hadoop.io.WritableComparable;

public class AtomWritable implements WritableComparable<OnDiskAtom> {
    private final OnDiskAtom.Serializer serializer = new OnDiskAtom.Serializer(new ColumnSerializer());
    private OnDiskAtom atom;
    private long deletedAt;
    private byte[] key;
    private String comparatorType;
    Comparator<ByteBuffer> comparator;

    public AtomWritable() {
    }

    public AtomWritable(byte[] key, long deletedAt, OnDiskAtom atom, String comparatorType) throws IOException {
        this.atom = atom;
        this.deletedAt = deletedAt;
        this.key = key;
        this.comparatorType = comparatorType;
        try {
            comparator = TypeParser.parse(comparatorType);
        } catch (SyntaxException e) {
            throw new IOException(e);
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void readFields(DataInput dis) throws IOException {
        int length = dis.readInt();
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        comparatorType = String.valueOf(bytes);
        try {
            //TODO: this is a little heavy, especially since this should always be the same.
            comparator = TypeParser.parse(comparatorType);
        } catch (SyntaxException e) {
            throw new IOException(e);
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
        length = dis.readInt();
        bytes = new byte[length];
        dis.readFully(bytes);
        this.key = bytes;
        this.deletedAt = dis.readLong();
        this.atom = serializer.deserializeFromSSTable(dis, Flag.PRESERVE_SIZE, Integer.MIN_VALUE, Version.CURRENT);
    }

    @Override
    public void write(DataOutput dos) throws IOException {
        dos.writeInt(comparatorType.length());
        dos.write(comparatorType.getBytes());
        dos.writeInt(this.key.length);
        dos.write(this.key);
        dos.writeLong(this.deletedAt);
        serializer.serializeForSSTable(this.atom, dos);
    }

    public OnDiskAtom getAtom() {
        return atom;
    }

    public long getDeletedAt() {
        return deletedAt;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public int compareTo(OnDiskAtom o) {
        return comparator.compare(this.atom.name(), o.name());
    }
}