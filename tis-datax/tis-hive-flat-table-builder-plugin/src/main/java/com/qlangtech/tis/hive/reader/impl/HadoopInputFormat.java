/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.hive.reader.impl;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.qlangtech.tis.config.hive.meta.IHiveTableParams;
import com.qlangtech.tis.hive.DefaultHiveMetaStore.HiveStoredAs;
import com.qlangtech.tis.plugin.datax.format.BasicPainFormat;
import com.qlangtech.tis.plugin.datax.format.TextFormat;
import com.qlangtech.tis.plugin.ds.CMeta;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-07 15:55
 * @see com.qlangtech.tis.hive.reader.HiveDFSLinker#getInputFileFormat Create By
 **/
public abstract class HadoopInputFormat<K, V extends Writable> extends TextFormat {
    private final org.apache.hadoop.mapred.InputFormat inputFormat;
    private final HiveOutputFormat outputFormat;
    protected final JobConf conf;
    private final String entityName;
    private final HiveStoredAs serde;
    protected final IHiveTableParams tableProperties;

    private final K key;
    private final V value;

    public HadoopInputFormat(String entityName, int colSize
            , HiveStoredAs serde, IHiveTableParams tableProperties, JobConf conf) {
        super();
        this.tableProperties = Objects.requireNonNull(tableProperties, "tableProperties can not be null");
        this.dateFormat = BasicPainFormat.defaultNullFormat();
        this.entityName = entityName;
        this.inputFormat = Objects.requireNonNull(serde.getInputFormat(), "inputFormat can not be null");
        this.outputFormat = Objects.requireNonNull(serde.getOutputFormat(), "outputFormat can not be null");
        this.conf = Objects.requireNonNull(conf, "conf can not be null");
        this.serde = Objects.requireNonNull(serde, "serde can not be null");
        this.key = this.createKey();
        this.value = this.createValue(colSize);
    }

    public final FileSinkOperator.RecordWriter createRecordsWriter(FileSystem fs, String path) {
        try {

            FileSinkOperator.RecordWriter recordWriter
                    = outputFormat.getHiveRecordWriter(conf, new Path(path)
                    , this.value.getClass(), isCompressed(), this.serde.getTabStoreProps(), Reporter.NULL);
            //  RecordWriter<K, V> recordWriter = outputFormat.getRecordWriter(fs, this.conf, path, Reporter.NULL);

            return recordWriter;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * TODO 在建表语句中设置了压缩开启的相关属性，在hdfs上导入使用压缩的文本，最终通过hive sql 添加了对应的分区，从在hive控制台上读取时，记录条数不一致且还有乱码
     *
     * @return
     */
    protected abstract boolean isCompressed();

    public abstract K createKey();

    public abstract V createValue(int colSize);

    public void iterateReadRecords(DataXCol2Index col2Index
            , List<ColumnEntry> colsMeta, Path inputPath, RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        //  Path inputPath = new Path(fileName);
        // FileInputFormat.addInputPath(job, inputPath);
        Objects.requireNonNull(colsMeta, "colsMeta can not be null");
        // try (RecordReader<LongWritable, Text> recordReader = inputFormat.getRecordReader(inputPath)) {
        // NullWritable, ArrayWritable
        RecordReader<K, V> recordReader = null;
        try {
            recordReader = this.getRecordReader(inputPath);
            V rowVal = recordReader.createValue();
            K rowKey = recordReader.createKey();

            Object row = null;
            StructObjectInspector inspector = (StructObjectInspector) this.getSerde().getObjectInspector();
            ColumnEntry columnEntry = null;
            Object fieldVal = null;
            final String[] parseRows = new String[colsMeta.size()];


//            NullWritable key = NullWritable.get();
//            ArrayWritable value = new ArrayWritable(Text.class, new Writable[colsMeta.size()]);

            while (recordReader.next(rowKey, rowVal)) {
                //
                row = this.getSerde().deserialize(rowVal);
                Arrays.fill(parseRows, null);
                for (int i = 0; i < colsMeta.size(); i++) {
                    columnEntry = colsMeta.get(i);
                    fieldVal = inspector.getStructFieldData(row, inspector.getStructFieldRef(columnEntry.getColName()));
                    if (fieldVal != null) {
                        parseRows[i] = String.valueOf(fieldVal);
                    }
                }

                UnstructuredStorageReaderUtil.transportOneRecord(col2Index, recordSender, colsMeta, parseRows, taskPluginCollector);

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                recordReader.close();
            } catch (Throwable e) {
            }
        }
    }


    public final String getEntityName() {
        return this.entityName;
    }

    private InputSplit[] getSplits() throws IOException {
        return inputFormat.getSplits(conf, 1);
    }

    public  org.apache.hadoop.hive.serde2.SerDe getSerde() {
        return this.serde.getSerde();
    }

    /**
     * example : LongWritable, Text
     *
     * @param inputPath
     * @return
     * @throws IOException
     */
    private RecordReader<K, V> getRecordReader(Path inputPath) throws IOException {
        FileInputFormat.addInputPath(conf, inputPath);
        InputSplit[] splits = this.getSplits();
        if (splits.length < 1) {
            throw new IllegalStateException("splits.length can not small 1");
        }
        return inputFormat.getRecordReader(splits[0], conf, Reporter.NULL);
    }

    @Override
    public UnstructuredWriter createWriter(Writer writer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnstructuredReader createReader(BufferedReader reader, List<CMeta> sourceCols) {
        throw new UnsupportedOperationException();
    }


}
