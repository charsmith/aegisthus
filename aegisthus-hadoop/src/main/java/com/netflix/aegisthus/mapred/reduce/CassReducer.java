/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.aegisthus.mapred.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.netflix.aegisthus.io.writable.ColumnWritable;
import com.netflix.aegisthus.message.AegisthusProtos.Column;
import com.netflix.aegisthus.message.AegisthusProtos.Row;
import com.netflix.aegisthus.tools.AegisthusSerializer;

public class CassReducer extends Reducer<Text, ColumnWritable, Text, Text> {
    public static class Reduce {
        Long deletedAt = Long.MIN_VALUE;
        Column currentColumn = null;
        Row.Builder rowBuilder;

        public Reduce() {
            rowBuilder = Row.newBuilder();
        }

        public void addColumn(Column column) {
            if (currentColumn == null) {
                currentColumn = column;
            } else if (!currentColumn.getColumnName().toStringUtf8().equals(column.getColumnName().toStringUtf8())) {
                rowBuilder.addColumn(currentColumn);
                currentColumn = column;
            } else if (currentColumn.getTimestamp() < column.getTimestamp()) {
                currentColumn = column;
            }
            if (column.getDeletedAt() > deletedAt) {
                deletedAt = column.getDeletedAt();
            }
        }

        public void finalize() {
            if (currentColumn != null) {
                rowBuilder.addColumn(currentColumn);
            }
            rowBuilder.setDeletedAt(deletedAt);
            if (currentColumn != null) {
                rowBuilder.setRowKey(currentColumn.getRowKey());
            }
        }
        
        public Row getRow() {
            return rowBuilder.build();
        }
    }

    Reduce reduce;

    public CassReducer() {
        reduce = new Reduce();
    }

    @Override
    public void reduce(Text key, Iterable<ColumnWritable> values, Context ctx) throws IOException, InterruptedException {
        for (ColumnWritable value : values) {
            reduce.addColumn(value.getColumn());
        }
        reduce.finalize();

        ctx.write(key, new Text(AegisthusSerializer.serialize(reduce.getRow())));
    }
}