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
package com.netflix.aegisthus.input.readers;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.netflix.aegisthus.input.splits.AegIndexedSplit;
import com.netflix.aegisthus.input.splits.AegSplit;
import com.netflix.aegisthus.io.sstable.IndexedSSTableScanner;
import com.netflix.aegisthus.io.sstable.SSTableScanner;
import com.netflix.aegisthus.io.writable.ColumnWritable;
import com.netflix.aegisthus.message.AegisthusProtos.Column;

public class SSTableRecordReader extends AegisthusRecordReader {
    private static final Log LOG = LogFactory.getLog(SSTableRecordReader.class);
    private SSTableScanner scanner;
    private String filename = null;
    private Iterator<Column> iterator = null;

    @Override
    public void close() throws IOException {
        super.close();
        if (scanner != null) {
            scanner.close();
        }
    }

    @Override
    public void initialize(InputSplit inputSplit, final TaskAttemptContext ctx) throws IOException,
            InterruptedException {
        AegSplit split = (AegSplit) inputSplit;

        start = split.getStart();
        InputStream is = split.getInput(ctx.getConfiguration());
        end = split.getDataEnd();
        filename = split.getPath().toUri().toString();

        LOG.info(String.format("File: %s", split.getPath().toUri().getPath()));
        LOG.info("Start: " + start);
        LOG.info("End: " + end);

        try {
            DataInput indexInput = null;
            if (inputSplit instanceof AegIndexedSplit) {
                AegIndexedSplit indexedSplit = (AegIndexedSplit) inputSplit;
                indexInput = new DataInputStream(indexedSplit.getIndexInput(ctx.getConfiguration()));
                scanner = new IndexedSSTableScanner(is, end, Descriptor.fromFilename(filename).version, indexInput);
            } else {
                scanner = new SSTableScanner(is, end, Descriptor.fromFilename(filename).version);
            }
            LOG.info("skipping to start: " + start);
            scanner.skipUnsafe(start);
            this.pos = start;
            LOG.info("Creating observable");
            iterator = scanner.observable()/*.onErrorFlatMap(new Func1<OnErrorThrowable, Observable<? extends Column>>() {
                @Override
                public Observable<? extends Column> call(OnErrorThrowable onErrorThrowable) {
                    LOG.error("failure deserializing", onErrorThrowable);
                    if (ctx instanceof TaskInputOutputContext) {
                        ((TaskInputOutputContext) ctx).getCounter("aegisthus",
                                onErrorThrowable.getCause().getClass().getSimpleName()).increment(1L);
                    }
                    return Observable.empty();
                }
            })*/
                    .toBlockingObservable()
                    .toIterable()
                    .iterator();
            LOG.info("done initializing");
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!iterator.hasNext()) {
            return false;
        }
        Column column = iterator.next();
        key.set(column.getRowKey().toStringUtf8());
        value = new ColumnWritable(column);
        return true;
    }
}