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
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.alibaba.datax.plugin.writer.hdfswriter.SupportHiveDataType;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.Lists;
import com.qlangtech.tis.config.hive.HiveUserToken;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TISCollectionUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.plugin.datax.TisDataXHdfsWriter;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiWriteTabType;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.web.start.TisAppLaunchPort;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-22 15:16
 **/
public class TisDataXHudiWriter extends HdfsWriter {

    private static final char CSV_Column_Separator = ',';
    private static final String CSV_NULL_VALUE = "null";
    private static final char CSV_ESCAPE_CHAR = '"';
    private static final boolean CSV_FILE_USE_HEADER = true;
    public static final String KEY_SOURCE_ORDERING_FIELD = "hudiSourceOrderingField";
    // public static final String KEY_SOURCE_ORDERING_FIELD = "hudiSourceOrderingField";
    private static final Logger logger = LoggerFactory.getLogger(TisDataXHudiWriter.class);

    public static class Job extends TisDataXHdfsWriter.Job {
        private String sourceOrderingField;
        private String dataXName;
        private DataXHudiWriter writerPlugin;
        private String pkName;
        private String partitionpathField;
        private Integer shuffleParallelism;
        //        private IPath fsSourcePropsPath;
//        private IPath fsSourceSchemaPath;
        // private FileSystemFactory fsFactory;
        private IPath tabDumpDir;
        private HudiWriteTabType hudiTabType;

        private IPath rootDir;


        @Override
        public void init() {
            super.init();
            this.sourceOrderingField
                    = this.cfg.getNecessaryValue(KEY_SOURCE_ORDERING_FIELD, HdfsWriterErrorCode.REQUIRED_VALUE);
            this.dataXName = this.cfg.getNecessaryValue(DataxUtils.DATAX_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
            this.pkName = cfg.getNecessaryValue("hudiRecordkey", HdfsWriterErrorCode.REQUIRED_VALUE);
            this.partitionpathField = cfg.getNecessaryValue("hudiPartitionpathField", HdfsWriterErrorCode.REQUIRED_VALUE);
            this.shuffleParallelism
                    = Integer.parseInt(cfg.getNecessaryValue("shuffleParallelism", HdfsWriterErrorCode.REQUIRED_VALUE));
            this.hudiTabType = HudiWriteTabType.parse(cfg.getNecessaryValue("hudiTabType", HdfsWriterErrorCode.REQUIRED_VALUE));

//            this.writerPlugin = getWriterPlugin();
            //  this.fsFactory = writerPlugin.getFs();
            //final String fsAddress = fsFactory.getFSAddress();
            // IPath rootDir = getRootPath(fsAddress);
            //IHiveConnGetter hiveConnGetter = getHiveConnGetter();

            // this.tabDumpDir = getDumpDir();


        }

        protected IPath getDumpDir() {
            if (this.tabDumpDir == null) {
                ITISFileSystem fs = this.getFileSystem();
                this.tabDumpDir = fs.getPath(getRootPath(), getHiveConnGetter().getDbName() + "/" + this.getFileName());
            }
            return this.tabDumpDir;
        }

        protected IPath getRootPath() {
            if (rootDir == null) {
                //  DataXHudiWriter writerPlugin = getHudiWriterPlugin();
                ITISFileSystem fs = this.getFileSystem();
                // Objects.requireNonNull(writerPlugin, "writerPlugin can not be null");
                Objects.requireNonNull(fs, "fileSystem can not be null");
                rootDir = fs.getRootDir();// fs.getPath(writerPlugin.getFs().getFSAddress() + );
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
            Objects.requireNonNull(fs, "ITISFileSystem can not be null");
            IPath tabDumpDir = getDumpDir();
            return fs.getPath(tabDumpDir, "data").unwrap(Path.class);
        }

        @Override
        public void post() {
            super.post();

            List<HdfsColMeta> colsMeta = HdfsColMeta.getColsMeta(this.cfg);

            DataXHudiWriter hudiPlugin = this.getHudiWriterPlugin();
            ITISFileSystem fs = this.getFileSystem();
            String tabName = this.getFileName();
            IPath fsSourcePropsPath = getSourcePropsPath();
            IPath fsSourceSchemaPath = fs.getPath(getDumpDir(), "meta/schema.avsc");

            try (FSDataOutputStream schemaWriter = this.hdfsHelper.getOutputStream(fsSourceSchemaPath.unwrap(Path.class))) {
                SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(this.getFileName());
                SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();

                for (HdfsColMeta meta : colsMeta) {
                    SupportHiveDataType hiveDataType = DataType.convert2HiveType(meta.type);
                    switch (hiveDataType) {
                        case STRING:
                        case DATE:
                        case TIMESTAMP:
                        case VARCHAR:
                        case CHAR:
                            // fields.nullableString(meta.colName, StringUtils.EMPTY);
//                            if (meta.nullable) {
//                                fields.nullableString(meta.colName, StringUtils.EMPTY);
//                            } else {
                            // fields.requiredString(meta.colName);
                            // SchemaBuilder.StringDefault<Schema> strType = fields.name(meta.colName).type().stringType();
                            if (meta.nullable) {
                                // strType.stringDefault(StringUtils.EMPTY);
                                fields.optionalString(meta.colName);
                            } else {
                                //   strType.noDefault();
                                fields.requiredString(meta.colName);
                            }
                            //}
                            break;
                        case DOUBLE:
                            if (meta.nullable) {
                                fields.optionalDouble(meta.colName);
                            } else {
                                fields.requiredDouble(meta.colName);
                            }
                            break;
                        case INT:
                        case TINYINT:
                        case SMALLINT:
                            if (meta.nullable) {
                                fields.optionalInt(meta.colName);
                            } else {
                                fields.requiredInt(meta.colName);
                            }
                            break;
                        case BOOLEAN:
                            if (meta.nullable) {
                                fields.optionalBoolean(meta.colName);
                            } else {
                                fields.requiredBoolean(meta.colName);
                            }
                            break;
                        case BIGINT:
                            if (meta.nullable) {
                                fields.optionalLong(meta.colName);
                            } else {
                                fields.requiredLong(meta.colName);
                            }
                            break;
                        case FLOAT:
                            if (meta.nullable) {
                                fields.optionalFloat(meta.colName);
                            } else {
                                fields.requiredFloat(meta.colName);
                            }
                            break;
                        default:
                            throw new IllegalStateException("illegal type:" + hiveDataType);
                    }
                }

                Schema schema = fields.endRecord();

                if (schema.getFields().size() != colsMeta.size()) {
                    throw new IllegalStateException("schema.getFields():" + schema.getFields().size() + " is not equal to 'colsMeta.size()':" + colsMeta.size());
                }
                IOUtils.write(schema.toString(true), schemaWriter, TisUTF8.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


            // 写csv文件属性元数据文件
            try (FSDataOutputStream write = this.hdfsHelper.getOutputStream(fsSourcePropsPath.unwrap(Path.class))) {
                // TypedProperties props = new TypedProperties();
                TypedPropertiesBuilder props = new TypedPropertiesBuilder();

                props.setProperty("hoodie.upsert.shuffle.parallelism", String.valueOf(this.shuffleParallelism));
                props.setProperty("hoodie.insert.shuffle.parallelism", String.valueOf(this.shuffleParallelism));
                props.setProperty("hoodie.delete.shuffle.parallelism", String.valueOf(this.shuffleParallelism));
                props.setProperty("hoodie.bulkinsert.shuffle.parallelism", String.valueOf(this.shuffleParallelism));
                props.setProperty("hoodie.embed.timeline.server", "true");
                props.setProperty("hoodie.filesystem.view.type", "EMBEDDED_KV_STORE");

                // @see HoodieCompactionConfig.INLINE_COMPACT
                // props.setProperty("hoodie.compact.inline", (hudiTabType == HudiWriteTabType.MOR) ? "true" : "false");
                // BasicFSWriter writerPlugin = this.getWriterPlugin();
//https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
                props.setProperty("hoodie.deltastreamer.source.dfs.root", String.valueOf(this.tabDumpParentPath));
                props.setProperty("hoodie.deltastreamer.csv.header", Boolean.toString(CSV_FILE_USE_HEADER));
                props.setProperty("hoodie.deltastreamer.csv.sep", String.valueOf(CSV_Column_Separator));
                props.setProperty("hoodie.deltastreamer.csv.nullValue", CSV_NULL_VALUE);
                props.setProperty("hoodie.deltastreamer.csv.escape", String.valueOf(CSV_ESCAPE_CHAR));
                //  props.setProperty("hoodie.deltastreamer.csv.escapeQuotes", "false");


                props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", String.valueOf(fsSourceSchemaPath));
                props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", String.valueOf(fsSourceSchemaPath));

                // please reference: DataSourceWriteOptions , HiveSyncConfig
                final IHiveConnGetter hiveMeta = getHiveConnGetter();
                props.setProperty("hoodie.datasource.hive_sync.database", hiveMeta.getDbName());
                props.setProperty("hoodie.datasource.hive_sync.table", tabName);
                if (StringUtils.isEmpty(hudiPlugin.partitionedBy)) {
                    throw new IllegalStateException("hudiPlugin.partitionedBy can not be empty");
                }
                props.setProperty("hoodie.datasource.hive_sync.partition_fields", hudiPlugin.partitionedBy);
                // "org.apache.hudi.hive.MultiPartKeysValueExtractor";
                // partition 分区值抽取类
                props.setProperty("hoodie.datasource.hive_sync.partition_extractor_class"
                        , "org.apache.hudi.hive.MultiPartKeysValueExtractor");

                Optional<HiveUserToken> hiveUserToken = hiveMeta.getUserToken();
                if (hiveUserToken.isPresent()) {
                    HiveUserToken token = hiveUserToken.get();
                    props.setProperty("hoodie.datasource.hive_sync.username", token.userName);
                    props.setProperty("hoodie.datasource.hive_sync.password", token.password);
                }
                props.setProperty("hoodie.datasource.hive_sync.jdbcurl", hiveMeta.getJdbcUrl());
                props.setProperty("hoodie.datasource.hive_sync.mode", "jdbc");

                props.setProperty("hoodie.datasource.write.recordkey.field", pkName);
                props.setProperty("hoodie.datasource.write.partitionpath.field", partitionpathField);


                props.store(write);

            } catch (IOException e) {
                throw new RuntimeException("faild to write " + this.tabDumpParentPath + " CSV file metaData", e);
            }

            try {
                this.launchSparkRddConvert();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private IPath getSourcePropsPath() {
            ITISFileSystem fs = this.getFileSystem();
            return fs.getPath(getDumpDir(), "meta/" + this.getFileName() + "-source.properties");
        }

        private void launchSparkRddConvert() throws Exception {

            // HashMap env = new HashMap();
            Map<String, String> env = Config.getInstance().getAllKV();


            String mdcCollection = MDC.get(TISCollectionUtils.KEY_COLLECTION);
            final String taskId = MDC.get(IParamContext.KEY_TASK_ID);
            env.put(IParamContext.KEY_TASK_ID, taskId);
            if (StringUtils.isNotEmpty(mdcCollection)) {
                env.put(TISCollectionUtils.KEY_COLLECTION, mdcCollection);
            }

            logger.info("environment props ===========================");
            for (Map.Entry<String, String> entry : env.entrySet()) {
                logger.info("key:{},value:{}", entry.getKey(), entry.getValue());
            }
            logger.info("=============================================");
            SparkLauncher handle = new SparkLauncher(env);
            File logFile = new File(TisAppLaunchPort.getAssebleTaskDir(), "full-" + taskId + ".log");
            FileUtils.touch(logFile);
            handle.redirectError(logFile);
            //  handle.redirectError(new File("error.log"));
            // handle.redirectToLog(DataXHudiWriter.class.getName());
            String tabName = this.getFileName();

            File hudiDependencyDir = HudiConfig.getHudiDependencyDir();
            File sparkHome = HudiConfig.getSparkHome();

            File resJar = FileUtils.listFiles(hudiDependencyDir, new String[]{"jar"}, false)
                    .stream().findFirst().orElseThrow(
                            () -> new IllegalStateException("must have resJar hudiDependencyDir:" + hudiDependencyDir.getAbsolutePath()));

            File addedJars = new File(hudiDependencyDir, "lib");
            boolean[] hasAddJar = new boolean[1];
            FileUtils.listFiles(addedJars, new String[]{"jar"}, false).forEach((jar) -> {
                handle.addJar(String.valueOf(jar.toPath().normalize()));
                hasAddJar[0] = true;
            });
            if (!hasAddJar[0]) {
                throw new IllegalStateException("path must contain jars:" + addedJars.getAbsolutePath());
            }
            //HudiWriteTabType tabType = writerPlugin.parseTabType();
            handle.setAppResource(String.valueOf(resJar.toPath().normalize()));
            // handle.addJar("/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-utilities_2.11-0.10.0.jar");
            ISparkConnGetter sparkConnGetter = writerPlugin.getSparkConnGetter();
            handle.setMaster(sparkConnGetter.getSparkMaster());
            handle.setSparkHome(String.valueOf(sparkHome.toPath().normalize()));
            handle.setMainClass("com.alibaba.datax.plugin.writer.hudi.TISHoodieDeltaStreamer");

            IPath fsSourcePropsPath = getSourcePropsPath();
            IPath hudiDataDir = this.getFileSystem().getPath(getDumpDir(), "hudi");
            handle.addAppArgs("--table-type", hudiTabType.getValue()
                    , "--source-class", "org.apache.hudi.utilities.sources.CsvDFSSource"
                    , "--source-ordering-field", this.sourceOrderingField
                    , "--target-base-path", String.valueOf(hudiDataDir)
                    , "--target-table", tabName + "/" + this.dataXName
                    , "--props", String.valueOf(fsSourcePropsPath)
                    , "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
                    , "--enable-sync"
            );

            if (this.hudiTabType == HudiWriteTabType.MOR) {
                handle.addAppArgs("--disable-compaction");
            }


            CountDownLatch countDownLatch = new CountDownLatch(1);
            // SparkAppHandle.State[] finalState = new SparkAppHandle.State[1];
            SparkAppHandle sparkAppHandle = handle.startApplication(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
//                    System.out.println(sparkAppHandle.getAppId());
//                    System.out.println("state:" + sparkAppHandle.getState().toString());
                    SparkAppHandle.State state = sparkAppHandle.getState();
                    if (state.isFinal()) {
                        // finalState[0] = state;
                        System.out.println("Info:" + sparkAppHandle.getState());
                        countDownLatch.countDown();
                    }
                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println("Info:" + sparkAppHandle.getState().toString());
                }
            });
            countDownLatch.await();
            if (sparkAppHandle.getState() != SparkAppHandle.State.FINISHED) {
                throw new TisException("spark app:" + sparkAppHandle.getAppId() + " execute result not successfule:" + sparkAppHandle.getState());
            }
        }
    }


    private static class TypedPropertiesBuilder {
        private List<String[]> props = Lists.newArrayList();

        public void setProperty(String key, String value) {
            props.add(new String[]{key, value});
        }

        public void store(FSDataOutputStream write) throws IOException {
            if (props.isEmpty()) {
                throw new IllegalStateException("props can not be null");
            }
            IOUtils.write(props.stream().map((prop) -> prop[0] + "=" + prop[1]).collect(Collectors.joining("\n")), write, TisUTF8.get());
        }
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

