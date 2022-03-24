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

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriter;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.plugin.datax.TisDataXHdfsWriter;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiDumpPostTask;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-22 15:16
 **/
public class TisDataXHudiWriter extends HdfsWriter {

    // public static final String KEY_SOURCE_ORDERING_FIELD = "hudiSourceOrderingField";
    private static final Logger logger = LoggerFactory.getLogger(TisDataXHudiWriter.class);
    public static final String KEY_HUDI_TABLE_NAME = "hudiTableName";

    public static class Job extends TisDataXHdfsWriter.Job {

        private HudiTableMeta tabMeta;

        private DataXHudiWriter writerPlugin;
        // private IPath tabDumpDir;
        private IPath rootDir;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            final String tabName = this.cfg.getNecessaryValue(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
            List<Configuration> cfgs = super.split(mandatoryNumber);
            for (Configuration cfg : cfgs) {
                cfg.set(KEY_HUDI_TABLE_NAME, tabName);
            }
            return cfgs;
        }


        @Override
        public void init() {
            getPluginJobConf();
            this.tabMeta = new HudiTableMeta(this.cfg);
            super.init();

        }

        @Override
        public void prepare() {
            super.prepare();

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
            return HudiDumpPostTask.createTabDumpParentPath(fs, getDumpDir()).unwrap(Path.class);
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
        // ObjectWriter csvObjWriter = null;
        // CustomCSVSchemaBuilder csvSchemaBuilder = null;
        private Schema avroSchema;
        private List<HdfsColMeta> colsMeta;
        private HudiTableMeta tabMeta;

        private DataFileWriter<GenericRecord> dataFileWriter;

        @Override
        public void init() {
            super.init();
            this.tabMeta = new HudiTableMeta(this.writerSliceConfig
                    , this.writerSliceConfig.getNecessaryValue(KEY_HUDI_TABLE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE));
            this.avroSchema = this.getAvroSchema();
        }

        protected Schema getAvroSchema() {
            ITISFileSystem fs = writerPlugin.getFs().getFileSystem();
            IPath tabSourceSchema = HudiTableMeta.getTableSourceSchema(
                    fs, this.tabMeta.getDumpDir(fs, ((DataXHudiWriter) writerPlugin).getHiveConnMeta()));
            try (InputStream reader = fs.open(tabSourceSchema)) {
                Objects.requireNonNull(reader, "schema reader can not be null");
                return new Schema.Parser().parse(reader);
            } catch (Exception e) {
                throw new RuntimeException(String.valueOf(tabSourceSchema), e);
            }
        }

        @Override
        public void prepare() {
            super.prepare();
            // HudiTableMeta.getTableSourceSchema(this.);
//            this.avroSchema = Objects.requireNonNull(
//                    this.writerSliceConfig.getAttr(KEY_AVRO_SCHEMA), "schema can not be null");


            this.colsMeta = HdfsColMeta.getColsMeta(this.writerSliceConfig);
            if (CollectionUtils.isEmpty(this.colsMeta)) {
                throw new IllegalStateException("colsMeta can not be empty");
            }

//            this.csvSchemaBuilder = new CustomCSVSchemaBuilder(); //CsvSchema.builder();
//            // this.csvSchemaBuilder.enableAlwaysQuoteStrings();
//
//            List<HdfsColMeta> colsMeta = HdfsColMeta.getColsMeta(this.writerSliceConfig);
//            for (HdfsColMeta col : colsMeta) {
//                csvSchemaBuilder.addColumn(col.colName, parseCsvType(col));
//            }
//            csvObjWriter = new CsvMapper().configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, true)
//                    .setSerializerFactory(new TISSerializerFactory(colsMeta))
//                    .writerFor(Record.class)
//                    .with(csvSchemaBuilder
//                            .setUseHeader(CSVWriter.CSV_FILE_USE_HEADER)
//                            .setColumnSeparator(CSVWriter.CSV_Column_Separator)
//                            .setNullValue(CSVWriter.CSV_NULL_VALUE)
//                            .setEscapeChar(CSVWriter.CSV_ESCAPE_CHAR).build());
        }


//        private CsvSchema.ColumnType parseCsvType(HdfsColMeta col) {
//            switch (col.csvType) {
//                case STRING:
//                    return CsvSchema.ColumnType.STRING;
//                case BOOLEAN:
//                    return CsvSchema.ColumnType.BOOLEAN;
//                case NUMBER:
//                    return CsvSchema.ColumnType.NUMBER;
//            }
//            throw new IllegalStateException("illegal csv type:" + col.csvType);
//        }


        @Override
        protected void avroFileStartWrite(RecordReceiver lineReceiver
                , Configuration config, String fileName, TaskPluginCollector taskPluginCollector) {
            try {
                Path targetPath = new Path(config.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME));
                //  DataFileWriter<GenericRecord> dataFileWriter = null;
                GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(this.avroSchema);
                datumWriter.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
                datumWriter.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
                datumWriter.getData().addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
                datumWriter.getData().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
                try (OutputStream output = getOutputStream(targetPath)) {
                    dataFileWriter = new DataFileWriter<>(datumWriter);
                    dataFileWriter = dataFileWriter.create(this.avroSchema, output);
                    Record record = null;
                    while ((record = lineReceiver.getFromReader()) != null) {
                        // sequenceWriter.write(record);
//                        GenericRecord user1 = new GenericData.Record(this.avroSchema);
//                        user1.put("name", "Alyssa");
//                        user1.put("favorite_number", 256);
                        dataFileWriter.append(convertAvroRecord(record));
                    }
                    dataFileWriter.flush();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void post() {

        }

        private GenericRecord convertAvroRecord(Record record) {
            GenericRecord r = new GenericData.Record(this.avroSchema);
            int i = 0;
            Column column = null;
            for (HdfsColMeta meta : colsMeta) {
                column = record.getColumn(i++);
                if (column.getRawData() == null) {
                    r.put(meta.getName(), null);
                    continue;
                }
                r.put(meta.getName(), parseAvroVal(meta, column));
            }

            return r;
        }

        private Object parseAvroVal(HdfsColMeta meta, Column colVal) {
            switch (meta.type.type) {
                case Types.TINYINT:
                case Types.INTEGER:
                case Types.SMALLINT:
                    return colVal.asBigInteger().intValue();
                case Types.BIGINT:
                    return colVal.asBigInteger().longValue();
                case Types.FLOAT:
                case Types.DOUBLE:
                    return colVal.asDouble();
                case Types.DECIMAL:
                    return colVal.asBigDecimal();
                case Types.DATE:
                    return colVal.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                case Types.TIME:
                    return colVal.asDate().toInstant().atZone(ZoneId.systemDefault()).toLocalTime();
                case Types.TIMESTAMP:
                    return colVal.asDate().toInstant();
                case Types.BIT:
                case Types.BOOLEAN:
                    return colVal.asBoolean();
                case Types.BLOB:
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    return ByteBuffer.wrap(colVal.asBytes());
                case Types.VARCHAR:
                case Types.LONGNVARCHAR:
                case Types.NVARCHAR:
                case Types.LONGVARCHAR:
                    // return visitor.varcharType(this);
                default:
                    return colVal.asString();// "VARCHAR(" + type.columnSize + ")";
            }
        }


        @Override
        protected void csvFileStartWrite(
                RecordReceiver lineReceiver, Configuration config
                , String fileName, TaskPluginCollector taskPluginCollector) {
//            try {
//
//
//                Path targetPath = new Path(config.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME));
//                try (OutputStream output = getOutputStream(targetPath)) {
//                    SequenceWriter sequenceWriter = csvObjWriter.writeValues(output);
//                    Record record = null;
//                    while ((record = lineReceiver.getFromReader()) != null) {
//                        sequenceWriter.write(record);
//                    }
//                }
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }

            throw new UnsupportedOperationException();
        }

        protected OutputStream getOutputStream(Path targetPath) {
            return this.hdfsHelper.getOutputStream(targetPath);
        }
    }


//    private static class CustomCSVSchemaBuilder extends CsvSchema.Builder {
//
//    }
}

