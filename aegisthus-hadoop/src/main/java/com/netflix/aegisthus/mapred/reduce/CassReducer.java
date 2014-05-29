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
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.netflix.aegisthus.io.writable.ColumnWritable;
import com.netflix.aegisthus.message.AegisthusProtos.Column;
import com.netflix.aegisthus.message.AegisthusProtos.Row;
import com.netflix.aegisthus.tools.AegisthusSerializer;

public class CassReducer extends Reducer<Text, ColumnWritable, Text, Text> {
    public static final AegisthusSerializer as = new AegisthusSerializer();

    protected static void insertKey(StringBuilder sb, Object value) {
        sb.append("\"");
        sb.append(value);
        sb.append("\": ");
    }

	protected static void serializeColumns(StringBuilder sb, List<Column> columns) {
		int count = 0;
		for (Column column: columns) {
			if (count++ > 0) {
				sb.append(", ");
			}
			sb.append("[");
			sb.append("\"").append(((String) column.getColumnName().toStringUtf8()).replace("\\", "\\\\").replace("\"", "\\\"")).append("\"").append(",");
			sb.append("\"").append(column.getValue().toStringUtf8()).append("\"").append(",");
			sb.append(column.getTimestamp());
			switch (column.getColumnType()) {
            case COUNTER:
				sb.append(",").append("\"c\"");
                break;
            case DELETED:
				sb.append(",").append("\"d\"");
				sb.append(",").append(column.getDeletedAt());
                break;
            case EXPIRING:
				sb.append(",").append("\"e\"");
				sb.append(",").append(column.getTtl());
                break;
            default:
                break;
            }
			sb.append("]");
		}
	}
    public static String serialize(Row row) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        insertKey(sb, row.getRowKey().toStringUtf8());
        sb.append("{");
        insertKey(sb, "deletedAt");
        sb.append(row.getDeletedAt());
        sb.append(", ");
        insertKey(sb, "columns");
        sb.append("[");
        serializeColumns(sb, row.getColumnList());
        sb.append("]");
        sb.append("}}");

        return sb.toString();
    }

    @Override
    public void reduce(Text key, Iterable<ColumnWritable> values, Context ctx) throws IOException, InterruptedException {
        Long deletedAt = Long.MIN_VALUE;
        Column currentColumn = null;
        Row.Builder rowBuilder = Row.newBuilder();
        for (ColumnWritable value : values) {
            if (currentColumn == null) {
                currentColumn = value.getColumn();
            } else if (!currentColumn.getColumnName().toStringUtf8().equals(value.getColumn().getColumnName().toStringUtf8())) {
                rowBuilder.addColumn(currentColumn);
                currentColumn = value.getColumn();
            } else if (currentColumn.getTimestamp() < value.getColumn().getTimestamp()) {
                currentColumn = value.getColumn();
            }
            if (value.getColumn().getDeletedAt() > deletedAt) {
                deletedAt = value.getColumn().getDeletedAt();
            }
        }
        if (currentColumn != null) {
            rowBuilder.addColumn(currentColumn);
        }
        rowBuilder.setDeletedAt(deletedAt);
        if (currentColumn != null) {
            rowBuilder.setRowKey(currentColumn.getRowKey());
        }

        ctx.write(key, new Text(serialize(rowBuilder.build())));
    }
}