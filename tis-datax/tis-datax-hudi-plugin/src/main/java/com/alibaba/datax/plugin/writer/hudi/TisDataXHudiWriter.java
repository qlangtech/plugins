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
import com.alibaba.datax.plugin.writer.hdfswriter.*;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.Lists;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.hdfs.test.HdfsFileSystemFactoryTestUtils;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.datax.BasicFSWriter;
import com.qlangtech.tis.plugin.datax.TisDataXHdfsWriter;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-22 15:16
 **/
public class TisDataXHudiWriter extends HdfsWriter {

    private static final char CSV_Column_Separator = ',';
    private static final boolean CSV_FILE_USE_HEADER = true;

    public static class Job extends TisDataXHdfsWriter.Job {
        @Override
        public void post() {
            super.post();

            List<ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType>>
                    colsMeta = HdfsHelper.getColsMeta(this.cfg);

            try (FSDataOutputStream schemaWriter = this.hdfsHelper.getOutputStream(HudiWriter.hdfs_source_schemaFilePath)) {
                SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(this.fileName);
                SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
                for (ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType> meta : colsMeta) {
                    switch (meta.middle) {
                        case STRING:
                        case DATE:
                        case TIMESTAMP:
                        case VARCHAR:
                        case CHAR:
                            fields.requiredString(meta.left);
                            break;
                        case DOUBLE:
                            fields.requiredDouble(meta.left);
                            break;
                        case INT:
                        case TINYINT:
                        case SMALLINT:
                            fields.requiredInt(meta.left);
                            break;
                        case BOOLEAN:
                            fields.requiredBoolean(meta.left);
                            break;
                        case BIGINT:
                            fields.requiredLong(meta.left);
                            break;
                        case FLOAT:
                            fields.requiredFloat(meta.left);
                            break;
                        default:
                            throw new IllegalStateException("illegal type:" + meta.middle);
                    }
                }
                Schema schema = fields.endRecord();
                IOUtils.write(schema.toString(true), schemaWriter, TisUTF8.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


            // 写csv文件属性元数据文件
            try (FSDataOutputStream write = this.hdfsHelper.getOutputStream(HudiWriter.hdfs_propsFilePath)) {
                // TypedProperties props = new TypedProperties();
                TypedPropertiesBuilder props = new TypedPropertiesBuilder();
                final String pkName = cfg.getNecessaryValue("hudiRecordkey", HdfsWriterErrorCode.REQUIRED_VALUE);
                final String partitionpathField = cfg.getNecessaryValue("hudiPartitionpathField", HdfsWriterErrorCode.REQUIRED_VALUE);
                props.setProperty("hoodie.upsert.shuffle.parallelism", "2");
                props.setProperty("hoodie.insert.shuffle.parallelism", "2");
                props.setProperty("hoodie.delete.shuffle.parallelism", "2");
                props.setProperty("hoodie.bulkinsert.shuffle.parallelism", "2");
                props.setProperty("hoodie.embed.timeline.server", "true");
                props.setProperty("hoodie.filesystem.view.type", "EMBEDDED_KV_STORE");
                props.setProperty("hoodie.compact.inline", "false");
                BasicFSWriter writerPlugin = this.getWriterPlugin();
                final String fsAddress = writerPlugin.getFs().getFSAddress();
                props.setProperty("hoodie.deltastreamer.source.dfs.root", fsAddress + this.tabDumpParentPath);
                props.setProperty("hoodie.deltastreamer.csv.header", Boolean.toString(CSV_FILE_USE_HEADER));
                props.setProperty("hoodie.deltastreamer.csv.sep", String.valueOf(CSV_Column_Separator));
                props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", HudiWriter.hdfs_source_schemaFilePath);
                props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", HudiWriter.hdfs_source_schemaFilePath);


                props.setProperty("hoodie.datasource.write.recordkey.field", pkName);
                props.setProperty("hoodie.datasource.write.partitionpath.field", partitionpathField);


                props.store(write);

            } catch (IOException e) {
                throw new RuntimeException("faild to write " + this.tabDumpParentPath + " CSV file metaData", e);
            }


        }

        private void launchSparkRddConvert() throws Exception {
            DataXHudiWriter writerPlugin = (DataXHudiWriter)this.getWriterPlugin();
            HashMap env = new HashMap();
            SparkLauncher handle = new SparkLauncher(env);

            File hudiLibDir = Config.getPluginLibDir("tis-datax-hudi-plugin");
            File hudiPluginDir = new File(hudiLibDir, "../..");
            File hudiDependencyDir = new File(hudiLibDir, "tis-datax-hudi-dependency");
            File sparkHome = new File(hudiPluginDir, HudiConfig.getSparkReleaseDir());
            if (!sparkHome.exists()) {
                throw new IllegalStateException("sparkHome is not exist:" + sparkHome.getAbsolutePath());
            }
            if (!hudiDependencyDir.exists()) {
                throw new IllegalStateException("hudiDependencyDir is not exist:" + hudiDependencyDir.getAbsolutePath());
            }

            File resJar = FileUtils.listFiles(hudiDependencyDir, new String[]{"jar"}, false)
                    .stream().findFirst().orElseThrow(() -> new IllegalStateException("must have resJar hudiDependencyDir:" + hudiDependencyDir.getAbsolutePath()));

            File addedJars = new File(hudiDependencyDir, "lib");
            boolean[] hasAddJar = new boolean[1];
            FileUtils.listFiles(addedJars, new String[]{"jar"}, false).forEach((jar) -> {
                handle.addJar(String.valueOf(jar.toPath().normalize()));
                hasAddJar[0] = true;
            });
            if (!hasAddJar[0]) {
                throw new IllegalStateException("path must contain jars:" + addedJars.getAbsolutePath());
            }

            handle.setAppResource(String.valueOf(resJar.toPath().normalize()));
            // handle.addJar("/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-hudi-plugin/target/tis-datax-hudi-plugin/WEB-INF/lib/hudi-utilities_2.11-0.10.0.jar");
            ISparkConnGetter sparkConnGetter = writerPlugin.getSparkConnGetter();
            handle.setMaster(sparkConnGetter.getSparkMaster());
            handle.setSparkHome(String.valueOf(sparkHome.toPath().normalize()));
            handle.setMainClass("com.alibaba.datax.plugin.writer.hudi.TISHoodieDeltaStreamer");
            handle.addAppArgs("--table-type", "COPY_ON_WRITE"
                    , "--source-class", "org.apache.hudi.utilities.sources.CsvDFSSource"
                    , "--source-ordering-field", "last_ver"
                    , "--target-base-path", "/user/hive/warehouse/customer_order_relation"
                    , "--target-table", "customer_order_relation/" + HdfsFileSystemFactoryTestUtils.testDataXName.getName()
                    , "--props", "/user/admin/customer_order_relation-source.properties"
                    , "--schemaprovider-class", "org.apache.hudi.utilities.schema.FilebasedSchemaProvider"
                    , "--enable-sync", String.valueOf(true));

            CountDownLatch countDownLatch = new CountDownLatch(1);

            handle.startApplication(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println(sparkAppHandle.getAppId());
                    System.out.println("state:" + sparkAppHandle.getState().toString());

                    if (sparkAppHandle.getState().isFinal()) {
                        countDownLatch.countDown();
                    }
                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    System.out.println("Info:" + sparkAppHandle.getState().toString());
                }
            });
            countDownLatch.await();
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
        CsvSchema.Builder csvSchemaBuilder = null;

        @Override
        public void init() {
            super.init();

        }

        @Override
        public void prepare() {
            super.prepare();
            this.csvSchemaBuilder = CsvSchema.builder();

            List<ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType>>
                    colsMeta = HdfsHelper.getColsMeta(this.writerSliceConfig);
            for (ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType> col : colsMeta) {
                csvSchemaBuilder.addColumn(col.left, parseCsvType(col));
            }
            csvObjWriter = new CsvMapper()
                    .setSerializerFactory(new TISSerializerFactory(colsMeta))
                    .writerFor(Record.class)
                    .with(csvSchemaBuilder.setUseHeader(CSV_FILE_USE_HEADER).setColumnSeparator(CSV_Column_Separator).build());
        }


        private CsvSchema.ColumnType parseCsvType(ImmutableTriple<String, SupportHiveDataType, HdfsHelper.CsvType> col) {
            switch (col.right) {
                case STRING:
                    return CsvSchema.ColumnType.STRING;
                case BOOLEAN:
                    return CsvSchema.ColumnType.BOOLEAN;
                case NUMBER:
                    return CsvSchema.ColumnType.NUMBER;
            }
            throw new IllegalStateException("illegal csv type:" + col.right);
        }


        @Override
        protected void csvFileStartWrite(
                RecordReceiver lineReceiver, Configuration config
                , String fileName, TaskPluginCollector taskPluginCollector) {
            try {
                Path targetPath = new Path(hdfsHelper.conf.getWorkingDirectory()
                        , this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE)
                        + "/" + this.fileName);
                FSDataOutputStream output = this.hdfsHelper.getOutputStream(targetPath);
                SequenceWriter sequenceWriter = csvObjWriter.writeValues(output);
                Record record = null;
                while ((record = lineReceiver.getFromReader()) != null) {
                    sequenceWriter.write(record);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    }
}

