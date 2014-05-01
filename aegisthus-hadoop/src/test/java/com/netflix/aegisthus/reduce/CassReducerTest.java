package com.netflix.aegisthus.reduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.netflix.aegisthus.io.writable.ColumnWritable;
import com.netflix.aegisthus.mapred.reduce.CassReducer;
import com.netflix.aegisthus.message.AegisthusProtos.Column;

public class CassReducerTest {
    private Text t(String text) {
        return new Text(text);
    }

    @DataProvider
    public Object[][] values() {
        return new Object[][] {

                {
                        "{\"uid\":",
                        new String[] {
                                "{\"uid\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"CELL\",\"00000002\",1312400480243000], [\"ENABLED\",\"59\",1312400420243004]]}}",
                                "{\"uid\": {\"deletedAt\": 10, \"columns\": [[\"CELL\",\"00000000\",1312400475129000], [\"ENABLED\",\"59\",1312400475129004]]}}",
                                "{\"uid\": {\"deletedAt\": 0, \"columns\": [[\"CELL\",\"00000000\",1312400473649000], [\"\\\\N\",\"59\",1312400533649004]]}}" },
                        "{\"uid\": {\"deletedAt\": 10, \"columns\": [[\"CELL\",\"00000002\",1312400480243000], [\"ENABLED\",\"59\",1312400475129004], [\"\\\\N\",\"59\",1312400533649004]]}}" },
                {
                        "{\"uid\":",
                        new String[] {
                                "{\"uid\": {\"deletedAt\": -9223372036854775808, \"columns\": [[\"cell\",\"00000002\",1]]}}",
                                "{\"uid\": {\"deletedAt\": 10, \"columns\": []}}" },
                        "{\"uid\": {\"deletedAt\": 10, \"columns\": []}}" }

        };

    }

    @SuppressWarnings({ "unchecked" })
    @Test(dataProvider = "values")
    public void reduce(String keyString, List<ColumnWritable> values, String result) throws IOException,
            InterruptedException {
        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, ColumnWritable, Text, Text>.Context context = support.createMock(Context.class);
        Text key = t(keyString);

        context.write(EasyMock.eq(key), EasyMock.eq(new Text(result)));

        CassReducer reducer = new CassReducer();
        support.replayAll();
        reducer.reduce(key, values, context);
        support.verifyAll();
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void reducekeys() throws IOException, InterruptedException {
        Text key1 = new Text("uid1");
        Text key2 = new Text("uid2");
        Text value1 = new Text(
                "{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"cell1\",\"00000001\",1312400480243001]]}}");
        Text value2 = new Text(
                "{\"uid2\": {\"deletedAt\": 2, \"columns\": [[\"cell2\",\"00000002\",1312400480243002]]}}");

        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, ColumnWritable, Text, Text>.Context context = support.createMock(Context.class);

        context.write(EasyMock.eq(key1), EasyMock.eq(new Text(value1.toString())));
        context.write(EasyMock.eq(key2), EasyMock.eq(new Text(value2.toString())));

        List<ColumnWritable> values1 = Lists.newArrayList();
        values1.add(new ColumnWritable(Column.newBuilder()
                .setRowKey(ByteString.copyFromUtf8("uid1"))
                .setDeletedAt(1)
                .setColumnName(ByteString.copyFromUtf8("cell1"))
                .setValue(ByteString.copyFromUtf8("00000001"))
                .setTimestamp(1312400480243001L)
                .build()));
        List<ColumnWritable> values2 = Lists.newArrayList();
        values2.add(new ColumnWritable(Column.newBuilder()
                .setRowKey(ByteString.copyFromUtf8("uid2"))
                .setDeletedAt(2)
                .setColumnName(ByteString.copyFromUtf8("cell2"))
                .setValue(ByteString.copyFromUtf8("00000002"))
                .setTimestamp(1312400480243002L)
                .build()));
        CassReducer reduce = new CassReducer();
        support.replayAll();
        reduce.reduce(key1, values1, context);
        reduce.reduce(key2, values2, context);

        support.verifyAll();
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void reference() throws IOException, InterruptedException {
        Text key1 = new Text("{\"uid1\"");

        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, ColumnWritable, Text, Text>.Context context = support.createMock(Context.class);

        context.write(
                EasyMock.eq(key1),
                EasyMock.eq(new Text(
                        "{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"cell1\",\"00000001\",1312400480243001], [\"cell2\",\"00000001\",1312400480243001]]}}")));

        CassReducer reduce = new CassReducer();
        support.replayAll();
        List<ColumnWritable> values1 = Lists.newArrayList();
        values1.add(new ColumnWritable(Column.newBuilder()
                .setRowKey(ByteString.copyFromUtf8("uid1"))
                .setDeletedAt(1)
                .setColumnName(ByteString.copyFromUtf8("cell1"))
                .setValue(ByteString.copyFromUtf8("00000001"))
                .setTimestamp(1312400480243001L)
                .build()));
        values1.add(new ColumnWritable(Column.newBuilder()
                .setRowKey(ByteString.copyFromUtf8("uid1"))
                .setDeletedAt(1)
                .setColumnName(ByteString.copyFromUtf8("cell2"))
                .setValue(ByteString.copyFromUtf8("00000001"))
                .setTimestamp(1312400480243001L)
                .build()));
        reduce.reduce(key1, values1, context);

        support.verifyAll();
    }

    /**
     * We must escape \ all the way through reducers, but due to the way that we
     * process sstables this only affects column names for now
     */
    /*
    @SuppressWarnings({ "unchecked" })
    @Test
    public void escapecharacters() throws IOException, InterruptedException {
        Text key1 = new Text("key1");
        Text value1 = new Text(
                "{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"\\\\N\",\"\",1312400480243002], [\"cell2\",\"00000001\",1312400480243002]]}}");
        EasyMockSupport support = new EasyMockSupport();
        Reducer<Text, Text, Text, Text>.Context context = support.createMock(Context.class);

        context.write(EasyMock.eq(key1), EasyMock.eq(value1));

        List<Text> values1 = new ArrayList<Text>();
        values1.add(value1);
        values1.add(new Text(
                "{\"uid1\": {\"deletedAt\": 1, \"columns\": [[\"\\\\N\",\"\",1312400480243002], [\"cell2\",\"\\\\N\",1312400480243001]]}}"));
        CassReducer reduce = new CassReducer();
        support.replayAll();
        reduce.reduce(key1, values1, context);

        support.verifyAll();
    }
    */
}
