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

package com.alibaba.datax.plugin.reader.hive;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.util.DataXCol2Index;
import com.alibaba.datax.plugin.reader.ftpreader.Key;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.datax.plugin.writer.tdfsreader.TDFSReader;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.hive.reader.DataXHiveReader;
import com.qlangtech.tis.hive.reader.HiveDFSLinker;
import com.qlangtech.tis.hive.reader.impl.HadoopInputFormat;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @see DataXHiveReader
 */
public class HiveReader extends TDFSReader {

    public static class Job extends TDFSReader.Job {

        /**
         * 获取一个独立PT下的所有相关文件
         *
         * @param cfg
         * @param dfsSession
         * @param entityName
         * @return
         */
        @Override
        protected List<String> getReaderPaths(Configuration cfg, ITDFSSession dfsSession, Optional<String> entityName) {

            HiveDFSLinker dfsLinker = (HiveDFSLinker) this.getDFSLinker();
            Hiveserver2DataSourceFactory dsFactory = dfsLinker.getDataSourceFactory();
            HiveTable hiveTable = dsFactory.metadata.createMetaStoreClient() //
                    .getTable(dsFactory.dbName, entityName.orElseThrow(() -> new IllegalArgumentException("entityName can not be null")));
            PartitionFilter ptFilter = ((DataXHiveReader) this.getReader()).ptFilter;
            // Optional<String> ptFilter = Optional.of("pt=" + HiveTable.KEY_PT_LATEST);
            //  List<String> pts = hiveTable.listPaths(ptFilter);

            List<String> result = hiveTable.listPaths(ptFilter);

//            List<String> result = pts.stream() //
//                    .map((pt) -> IPath.pathConcat(hiveTable.getStorageLocation(), pt)).collect(Collectors.toList());

            cfg.set(Key.PATH, result);
            return result;
        }
    }

    public static class Task extends TDFSReader.Task {

        @Override
        protected void transformReocrds2Sender(DataXCol2Index col2Index, RecordSender recordSender, String fileName) {

            // HiveDFSLinker dfsLinker = (HiveDFSLinker) this.getDFSLinker();
            //   org.apache.hadoop.conf.Configuration conf = dfsLinker.getFs().getConfiguration();
            HadoopInputFormat inputFormat = this.getFileFormat();

            // 创建 JobConf 对象
//            JobConf job = new JobConf(conf);

            // 指定输入路径
            Path inputPath = new Path(fileName);
            // FileInputFormat.addInputPath(job, inputPath);

            try (RecordReader<LongWritable, Text> recordReader = inputFormat.getRecordReader(inputPath)) {
                LongWritable key = new LongWritable();
                Text value = new Text();
                Object row = null;
                StructObjectInspector inspector = (StructObjectInspector) inputFormat.getSerde().getObjectInspector();
                List<ColumnEntry> colsMeta = createColsMeta(Optional.of(inputFormat.getEntityName()));
                ColumnEntry columnEntry = null;
                Object fieldVal = null;
                final String[] parseRows = new String[colsMeta.size()];
                while (recordReader.next(key, value)) {
                    //
                    row = inputFormat.getSerde().deserialize(value);
                    Arrays.fill(parseRows, null);
                    for (int i = 0; i < colsMeta.size(); i++) {
                        columnEntry = colsMeta.get(i);
                        fieldVal = inspector.getStructFieldData(row, inspector.getStructFieldRef(columnEntry.getColName()));
                        if (fieldVal != null) {
                            parseRows[i] = String.valueOf(fieldVal);
                        }
                    }

                    UnstructuredStorageReaderUtil.transportOneRecord(col2Index, recordSender, colsMeta, parseRows, this.getTaskPluginCollector());
                    System.out.println("Key: " + key.get() + ", Value: " + value);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


//            // 获取输入分割
//            InputSplit[] split = inputFormat.getSplits() // FileInputFormat.getSplits(job, 1)[0];
//
//            // 创建 RecordReader
//            RecordReader<LongWritable, Text> recordReader = new LineRecordReader(job, (FileSplit) split);
//
//            try {
//                // 初始化 RecordReader
//                recordReader.initialize(split, job);
//
//                LongWritable key = new LongWritable();
//                Text value = new Text();
//
//                // 遍历所有记录
//                while (recordReader.next(key, value)) {
//                    System.out.println("Key: " + key.get() + ", Value: " + value.toString());
//                }
//            } finally {
//                // 关闭 RecordReader
//                recordReader.close();
//            }


        }

    }
}
