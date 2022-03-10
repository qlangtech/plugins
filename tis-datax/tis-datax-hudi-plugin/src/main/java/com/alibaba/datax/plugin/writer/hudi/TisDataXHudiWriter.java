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

package com.alibaba.datax.plugin.writer.hudi;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriter;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.plugin.datax.TisDataXHdfsWriter;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-22 15:16
 **/
public class TisDataXHudiWriter extends HdfsWriter {

    public static final char CSV_Column_Separator = ',';
    public static final String CSV_NULL_VALUE = "null";
    public static final char CSV_ESCAPE_CHAR = '"';
    public static final boolean CSV_FILE_USE_HEADER = true;

    public static IPath createTabDumpParentPath(ITISFileSystem fs, IPath tabDumpDir) {
        Objects.requireNonNull(fs, "ITISFileSystem can not be null");
        //IPath tabDumpDir = getDumpDir();
        return fs.getPath(tabDumpDir, "data");
    }

    // public static final String KEY_SOURCE_ORDERING_FIELD = "hudiSourceOrderingField";
    private static final Logger logger = LoggerFactory.getLogger(TisDataXHudiWriter.class);

    public static class Job extends TisDataXHdfsWriter.Job {

        private HudiTableMeta tabMeta;

        private DataXHudiWriter writerPlugin;
        private IPath tabDumpDir;
        private IPath rootDir;


        @Override
        public void init() {
            super.init();
            this.tabMeta = new HudiTableMeta(this.cfg);
        }


        protected IPath getRootPath() {
            if (rootDir == null) {
                ITISFileSystem fs = this.getFileSystem();
                Objects.requireNonNull(fs, "fileSystem can not be null");
                rootDir = fs.getRootDir();
            }
            return rootDir;
        }

        private DataXHudiWriter getHudiWriterPlugin() {
            if (this.writerPlugin == null) {
                this.writerPlugin = (DataXHudiWriter) this.getWriterPlugin();
            }
            return this.writerPlugin;
        }

        private IHiveConnGetter getHiveConnGetter() {
            return getHudiWriterPlugin().getHiveConnMeta();
        }

        protected Path createTabDumpParentPath(ITISFileSystem fs) {
            return TisDataXHudiWriter.createTabDumpParentPath(fs, getDumpDir()).unwrap(Path.class);
//            Objects.requireNonNull(fs, "ITISFileSystem can not be null");
//            IPath tabDumpDir = getDumpDir();
//            return fs.getPath(tabDumpDir, "data").unwrap(Path.class);
        }

        @Override
        public void post() {
            super.post();


//            DataXHudiWriter hudiPlugin = this.getHudiWriterPlugin();
//            ITISFileSystem fs = this.getFileSystem();
//            String tabName = this.getFileName();
//            IPath fsSourcePropsPath = getSourcePropsPath();
//
//            IPath fsSourceSchemaPath = HudiTableMeta.createFsSourceSchema(
//                    fs, this.getHiveConnGetter(), this.getFileName(), this.getDumpTimeStamp(), this.tabMeta);
//
//            // 写csv文件属性元数据文件
//            try (FSDataOutputStream write = this.hdfsHelper.getOutputStream(fsSourcePropsPath.unwrap(Path.class))) {
//                // TypedProperties props = new TypedProperties();
//                TypedPropertiesBuilder props = new TypedPropertiesBuilder();
//
//                String shuffleParallelism = String.valueOf(this.tabMeta.getShuffleParallelism());
//                props.setProperty("hoodie.upsert.shuffle.parallelism", shuffleParallelism);
//                props.setProperty("hoodie.insert.shuffle.parallelism", (shuffleParallelism));
//                props.setProperty("hoodie.delete.shuffle.parallelism", (shuffleParallelism));
//                props.setProperty("hoodie.bulkinsert.shuffle.parallelism", (shuffleParallelism));
//                props.setProperty("hoodie.embed.timeline.server", "true");
//                props.setProperty("hoodie.filesystem.view.type", "EMBEDDED_KV_STORE");
//
//                // @see HoodieCompactionConfig.INLINE_COMPACT
//                // props.setProperty("hoodie.compact.inline", (hudiTabType == HudiWriteTabType.MOR) ? "true" : "false");
//                // BasicFSWriter writerPlugin = this.getWriterPlugin();
////https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
//                props.setProperty("hoodie.deltastreamer.source.dfs.root", String.valueOf(this.tabDumpParentPath));
//                props.setProperty("hoodie.deltastreamer.csv.header", Boolean.toString(CSV_FILE_USE_HEADER));
//                props.setProperty("hoodie.deltastreamer.csv.sep", String.valueOf(CSV_Column_Separator));
//                props.setProperty("hoodie.deltastreamer.csv.nullValue", CSV_NULL_VALUE);
//                props.setProperty("hoodie.deltastreamer.csv.escape", String.valueOf(CSV_ESCAPE_CHAR));
//                //  props.setProperty("hoodie.deltastreamer.csv.escapeQuotes", "false");
//
//
//                props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", String.valueOf(fsSourceSchemaPath));
//                props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", String.valueOf(fsSourceSchemaPath));
//
//                // please reference: DataSourceWriteOptions , HiveSyncConfig
//                final IHiveConnGetter hiveMeta = getHiveConnGetter();
//                props.setProperty("hoodie.datasource.hive_sync.database", hiveMeta.getDbName());
//                props.setProperty("hoodie.datasource.hive_sync.table", tabName);
//                if (StringUtils.isEmpty(hudiPlugin.partitionedBy)) {
//                    throw new IllegalStateException("hudiPlugin.partitionedBy can not be empty");
//                }
//                props.setProperty("hoodie.datasource.hive_sync.partition_fields", hudiPlugin.partitionedBy);
//                // "org.apache.hudi.hive.MultiPartKeysValueExtractor";
//                // partition 分区值抽取类
//                props.setProperty("hoodie.datasource.hive_sync.partition_extractor_class"
//                        , "org.apache.hudi.hive.MultiPartKeysValueExtractor");
//
//                Optional<HiveUserToken> hiveUserToken = hiveMeta.getUserToken();
//                if (hiveUserToken.isPresent()) {
//                    HiveUserToken token = hiveUserToken.get();
//                    props.setProperty("hoodie.datasource.hive_sync.username", token.userName);
//                    props.setProperty("hoodie.datasource.hive_sync.password", token.password);
//                }
//                props.setProperty("hoodie.datasource.hive_sync.jdbcurl", hiveMeta.getJdbcUrl());
//                props.setProperty("hoodie.datasource.hive_sync.mode", "jdbc");
//
//                props.setProperty("hoodie.datasource.write.recordkey.field", tabMeta.getPkName());
//                props.setProperty("hoodie.datasource.write.partitionpath.field", tabMeta.getPartitionpathField());
//
//
//                props.store(write);
//
//            } catch (IOException e) {
//                throw new RuntimeException("faild to write " + this.tabDumpParentPath + " CSV file metaData", e);
//            }

//            try {
//                this.launchSparkRddConvert();
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
        }

//        protected static IPath createFsSourceSchema(ITISFileSystem fs, String tabName, String dumpTimeStamp, HudiTableMeta hudiTabMeta) {
//            return createFsSourceSchema(fs, tabName, dumpTimeStamp, hudiTabMeta.colMetas);
//        }

        protected IPath getDumpDir() {
            // this.getDumpTimeStamp()
            return this.tabMeta.getDumpDir(this, this.getHiveConnGetter());
        }

//        private IPath getSourcePropsPath() {
//            ITISFileSystem fs = this.getFileSystem();
//            return fs.getPath(getDumpDir(), "meta/" + this.getFileName() + "-source.properties");
//        }


    }


    public static class Task extends TisDataXHdfsWriter.Task {

        ObjectWriter csvObjWriter = null;
        CustomCSVSchemaBuilder csvSchemaBuilder = null;

        @Override
        public void init() {
            super.init();

        }

        @Override
        public void prepare() {
            super.prepare();
            this.csvSchemaBuilder = new CustomCSVSchemaBuilder(); //CsvSchema.builder();
            // this.csvSchemaBuilder.enableAlwaysQuoteStrings();

            List<HdfsColMeta> colsMeta = HdfsColMeta.getColsMeta(this.writerSliceConfig);
            for (HdfsColMeta col : colsMeta) {
                csvSchemaBuilder.addColumn(col.colName, parseCsvType(col));
            }
            csvObjWriter = new CsvMapper().configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, true)
                    .setSerializerFactory(new TISSerializerFactory(colsMeta))
                    .writerFor(Record.class)
                    .with(csvSchemaBuilder
                            .setUseHeader(CSV_FILE_USE_HEADER)
                            .setColumnSeparator(CSV_Column_Separator)
                            .setNullValue(CSV_NULL_VALUE)
                            .setEscapeChar(CSV_ESCAPE_CHAR).build());
        }


        private CsvSchema.ColumnType parseCsvType(HdfsColMeta col) {
            switch (col.csvType) {
                case STRING:
                    return CsvSchema.ColumnType.STRING;
                case BOOLEAN:
                    return CsvSchema.ColumnType.BOOLEAN;
                case NUMBER:
                    return CsvSchema.ColumnType.NUMBER;
            }
            throw new IllegalStateException("illegal csv type:" + col.csvType);
        }


        @Override
        protected void csvFileStartWrite(
                RecordReceiver lineReceiver, Configuration config
                , String fileName, TaskPluginCollector taskPluginCollector) {
            try {


                Path targetPath = new Path(config.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME));
//                Path targetPath = new Path(hdfsHelper.conf.getWorkingDirectory()
//                        , this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE)
//                        + "/" + this.fileName);
                try (OutputStream output = getOutputStream(targetPath)) {
                    SequenceWriter sequenceWriter = csvObjWriter.writeValues(output);
                    Record record = null;
                    while ((record = lineReceiver.getFromReader()) != null) {
                        sequenceWriter.write(record);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        protected OutputStream getOutputStream(Path targetPath) {
            return this.hdfsHelper.getOutputStream(targetPath);
        }
    }


    private static class CustomCSVSchemaBuilder extends CsvSchema.Builder {

    }
}

