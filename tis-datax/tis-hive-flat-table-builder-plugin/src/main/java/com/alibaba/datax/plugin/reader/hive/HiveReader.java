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
import com.alibaba.datax.plugin.writer.tdfsreader.TDFSReader;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.hive.reader.DataXHiveReader;
import com.qlangtech.tis.hive.reader.HiveDFSLinker;
import com.qlangtech.tis.hive.reader.impl.HadoopInputFormat;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Optional;

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
            List<ColumnEntry> colsMeta = createColsMeta(Optional.of(inputFormat.getEntityName()));
            // 指定输入路径
            Path inputPath = new Path(fileName);
            // FileInputFormat.addInputPath(job, inputPath);

            // try (RecordReader<LongWritable, Text> recordReader = inputFormat.getRecordReader(inputPath)) {

            inputFormat.iterateReadRecords(col2Index
                    , colsMeta, inputPath, recordSender, this.getTaskPluginCollector());

        }

    }
}
