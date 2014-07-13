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
package com.netflix.aegisthus.tools;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import rx.Observable;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Func1;

import com.google.protobuf.ByteString;
import com.netflix.aegisthus.io.sstable.IndexedSSTableScanner;
import com.netflix.aegisthus.io.sstable.OffsetScanner;
import com.netflix.aegisthus.io.sstable.SSTableScanner;
import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import com.netflix.aegisthus.mapred.reduce.CassReducer.Reduce;
import com.netflix.aegisthus.message.AegisthusProtos.Column;

public class SSTableExport {

    private static CommandLine cmd;
    private static final String END = "e";
    private static final String INDEX = "i";
    private static final String INDEX_SPLIT = "p";
    private static Options options = new Options();
    private static final String ROWSIZE = "r";
    private static final String COLUMN_NAME_TYPE = "c";
    private static final String OPT_COMP = "comp";
    private static final String OPT_VERSION = "v";
    private static final String OPT_INDEX_CHECK = "ic";

    static {
        options.addOption(new Option(ROWSIZE, false, "Output row sizes"));
        options.addOption(new Option(INDEX, false, "Process Index File"));
        options.addOption(new Option(INDEX_SPLIT, false, "Process Index File, and create input for split"));

        Option optCompression = new Option(OPT_COMP, true, "Compression file");
        optCompression.setArgs(1);
        options.addOption(optCompression);

        Option optColumnNameType = new Option(COLUMN_NAME_TYPE, true,
                "String indicating columns name types (AsciiType)");
        optColumnNameType.setArgs(1);
        options.addOption(optColumnNameType);

        Option optIndexCheck = new Option(OPT_INDEX_CHECK, true, "index file for the sstable");
        optIndexCheck.setArgs(1);
        options.addOption(optIndexCheck);

        Option optVersion = new Option(OPT_VERSION, true, "file version (e.g ic or hf)");
        optVersion.setArgs(1);
        options.addOption(optVersion);

        Option optEnd = new Option(END, true, "byte offset to end reading at");
        optEnd.setArgs(1);
        options.addOption(optEnd);
    }

    @SuppressWarnings("rawtypes")
    public static void export(Iterator scanner) throws IOException {
        // export(scanner, false);
    }

    @SuppressWarnings("rawtypes")
    public static void export(rx.Observable<Column> observable, final Map<String, AbstractType> convertors)
            throws IOException {
        observable.onErrorFlatMap(new Func1<OnErrorThrowable, Observable<? extends Column>>() {
            @Override
            public Observable<? extends Column> call(OnErrorThrowable onErrorThrowable) {
                System.err.println(onErrorThrowable.getMessage());
                return Observable.empty();
            }
        }).take(10).subscribe(new rx.Observer<Column>() {
            @Override
            public void onCompleted() {
                if (reduce != null) {
                    reduce.finalize();
                    System.out.println(AegisthusSerializer.serialize(reduce.getRow()));
                }
            }

            @Override
            public void onError(Throwable e) {
                System.err.println(e.getMessage());
            }

            ByteString curString = null;
            Reduce reduce = null;

            @Override
            public void onNext(Column t) {
                if (curString == null) {
                    curString = t.getColumnName();
                } else if (!curString.equals(t.getColumnName())) {
                    reduce.finalize();
                    System.out.println(AegisthusSerializer.serialize(reduce.getRow()));
                    curString = t.getColumnName();
                    reduce = null;
                }
                if (reduce == null) {
                    reduce = new Reduce();
                }
                reduce.addColumn(t);

            }
        });
    }

    public static void exportIndex(String ssTableFile) throws IOException {
        // export(new OffsetScanner(ssTableFile), true);
    }

    public static void exportIndexSplit(String ssTableFile, DataInput input, Descriptor.Version version)
            throws IOException {
        Iterator<Pair<Long, Long>> scanner = new OffsetScanner(input, version);

        long maxSplitSize = Long.getLong("aegisthus.block.size", 67108864);
        long splitStart = 0;
        while (scanner.hasNext()) {
            long splitSize = 0;
            // The scanner returns an offset from the start of the file.
            while (splitSize < maxSplitSize && scanner.hasNext()) {
                splitSize = scanner.next().left - splitStart;
            }
            if (scanner.hasNext()) {
                System.out.println(ssTableFile + "\t" + splitStart + "\t" + splitSize);
            } else {
                System.out.println(ssTableFile + "\t" + splitStart + "\t" + -1);
            }
            splitStart += splitSize;
        }
    }

    public static void exportRowSize(String ssTableFile) throws IOException {
        // export(new SSTableSplitScanner(ssTableFile), true);
    }

    public static void exportStream(Descriptor.Version version) throws IOException {
        export(new SSTableScanner(new DataInputStream(System.in), version).observable(), null);
    }

    @SuppressWarnings("rawtypes")
    public static void main(String[] args) throws IOException {
        String usage = String.format("Usage: %s <sstable>", SSTableExport.class.getName());

        CommandLineParser parser = new PosixParser();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e1) {
            System.err.println(e1.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(usage, options);
            System.exit(1);
        }

        if (cmd.getArgs().length != 1) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(usage, options);
            System.exit(1);
        }
        Map<String, AbstractType> convertors = null;
        if (cmd.hasOption(COLUMN_NAME_TYPE)) {
            try {
                convertors = new HashMap<String, AbstractType>();
                convertors.put(SSTableScanner.COLUMN_NAME_KEY, TypeParser.parse(cmd.getOptionValue(COLUMN_NAME_TYPE)));
            } catch (ConfigurationException e) {
                System.err.println(e.getMessage());
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(usage, options);
                System.exit(1);
            } catch (SyntaxException e) {
                System.err.println(e.getMessage());
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(usage, options);
                System.exit(1);
            }
        }
        Descriptor.Version version = null;
        if (cmd.hasOption(OPT_VERSION)) {
            version = new Descriptor.Version(cmd.getOptionValue(OPT_VERSION));
        }

        if (cmd.hasOption(INDEX_SPLIT)) {
            String ssTableFileName;
            DataInput input = null;
            if ("-".equals(cmd.getArgs()[0])) {
                ssTableFileName = System.getProperty("aegisthus.file.name");
                input = new DataInputStream(new BufferedInputStream(System.in, 65536 * 10));
            } else {
                ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();
                input = new DataInputStream(new BufferedInputStream(new FileInputStream(ssTableFileName), 65536 * 10));
            }
            if (version == null) {
                version = Descriptor.fromFilename(ssTableFileName).version;
            }
            exportIndexSplit(ssTableFileName, input, version);
        } else if (cmd.hasOption(INDEX)) {
            String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();
            exportIndex(ssTableFileName);
        } else if (cmd.hasOption(ROWSIZE)) {
            String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();
            exportRowSize(ssTableFileName);
        } else if ("-".equals(cmd.getArgs()[0])) {
            if (version == null) {
                System.err.println("when streaming must supply file version");
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(usage, options);
                System.exit(1);
            }
            exportStream(version);
        } else {
            String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();
            FileInputStream fis = new FileInputStream(ssTableFileName);
            InputStream inputStream = new DataInputStream(new BufferedInputStream(fis, 65536 * 10));
            long end = -1;
            if (cmd.hasOption(END)) {
                end = Long.valueOf(cmd.getOptionValue(END));
            }
            if (cmd.hasOption(OPT_COMP)) {
                CompressionMetadata cm = new CompressionMetadata(new BufferedInputStream(new FileInputStream(
                        cmd.getOptionValue(OPT_COMP)), 65536), fis.getChannel().size());
                inputStream = new CompressionInputStream(inputStream, cm);
                end = cm.getDataLength();
            }
            DataInputStream input = new DataInputStream(inputStream);
            if (version == null) {
                version = Descriptor.fromFilename(ssTableFileName).version;
            }
            DataInputStream inputIndex = null;
            SSTableScanner scanner = null;
            if (cmd.hasOption(OPT_INDEX_CHECK)) {
                FileInputStream fisIndex = new FileInputStream(cmd.getOptionValue(OPT_INDEX_CHECK));
                inputIndex = new DataInputStream(new BufferedInputStream(fisIndex, 65536 * 10));
                scanner = new IndexedSSTableScanner(input, end, version, inputIndex);
            } else {
                scanner = new SSTableScanner(input, end, version);
            }
            export(scanner.observable(), convertors);
        }
    }
}
