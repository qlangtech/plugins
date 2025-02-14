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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.FileFormatUtils;
import com.alibaba.datax.plugin.writer.hdfswriter.FileFormatUtils.ColumnTypeValInspectors;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsHelper;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.alibaba.datax.plugin.writer.hdfswriter.Key;
import com.alibaba.datax.plugin.writer.hdfswriter.TextFileUtils;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.HiveTable.HiveTabColType;
import com.qlangtech.tis.hive.DefaultHiveMetaStore.HiveStoredAs;
import com.qlangtech.tis.hive.HdfsFileType;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.hive.reader.SupportedFileFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopInputFormat;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 16:55
 **/
public class TisDataXHiveWriter extends Writer {

    public static final String KEY_HIVE_TAB_NAME = "hiveTableName";


    static final Logger logger = LoggerFactory.getLogger(TisDataXHiveWriter.class);

    public static class Job extends BasicEngineJob<DataXHiveWriter> {

    }

    public static class Task extends BasicDataXHdfsWriter.Task {

        IDataSourceFactoryGetter dsFacatoryGetter = null;
        private EntityName entityName = null;

        @Override
        public void init() {
            super.init();
            this.dsFacatoryGetter = (IDataSourceFactoryGetter) this.writerPlugin;
            this.entityName = createDumpTable();
        }

        private EntityName createDumpTable() {
            String hiveTableName = writerSliceConfig.getString(TisDataXHiveWriter.KEY_HIVE_TAB_NAME);
            if (StringUtils.isBlank(hiveTableName)) {
                throw new IllegalStateException("config key " + TisDataXHiveWriter.KEY_HIVE_TAB_NAME + " can not be null");
            }
//        if (!(writerPlugin instanceof DataXHiveWriter)) {
//            throw new IllegalStateException("hiveWriterPlugin must be type of DataXHiveWriter");
//        }
            return EntityName.create(getDataSourceFactory().getDbName(), hiveTableName);
        }

        @Override
        protected void orcFileStartWrite(FileSystem fileSystem, JobConf conf
                , RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector) {
            FileFormatUtils.orcFileStartWrite(fileSystem, conf, lineReceiver, config, fileName, taskPluginCollector);
        }

        @Override
        protected void startTextWrite(HdfsHelper fsHelper, RecordReceiver lineReceiver
                , Configuration config, String fileName, TaskPluginCollector taskPluginCollector) {
            TextFileUtils.startTextWrite(fsHelper, lineReceiver, config, fileName, taskPluginCollector);
        }

        private Hiveserver2DataSourceFactory getDataSourceFactory() {
            return (Hiveserver2DataSourceFactory) this.dsFacatoryGetter.getDataSourceFactory();
        }


        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            //super.startWrite(lineReceiver);

            HdfsHelper fsHelper = this.createHdfsHelper();

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
            String attempt = "attempt_" + dateFormat.format(new java.util.Date()) + "_0001_m_000000_0";
            Path outputPath = new Path(fileName);
            fsHelper.conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
            FileOutputFormat.setOutputPath(fsHelper.conf, outputPath);
            FileOutputFormat.setWorkOutputPath(fsHelper.conf, outputPath);

            HdfsFileType type = HdfsFileType.parse(fileType);
            HiveTable hiveTab = getDataSourceFactory().getHiveTableMeta(
                    Objects.requireNonNull(this.entityName, "entityName can not be null").getTabName());
            SupportedFileFormat fileFormat = null;
            switch (type) {
                case TEXTFILE: {
                    fileFormat = SupportedFileFormat.parse(() -> SupportedFileFormat.KEY_SUPPORTED_FORMAT_TEXT);
                    break;
                }
                case PARQUET: {
                    fileFormat = SupportedFileFormat.parse(() -> SupportedFileFormat.KEY_SUPPORTED_FORMAT_PARQUET);
                    break;
                }
                default:
                    throw new IllegalStateException("illegal file type:" + fileType);
            }


            try {
                HiveStoredAs storeConfig = (HiveStoredAs) hiveTab.getStoredAs(fsHelper.conf, org.apache.hadoop.hive.serde2.AbstractSerDe.class.getClassLoader());
                List<HiveTabColType> cols = hiveTab.getCols();

                HadoopInputFormat outputFileFormat
                        = (HadoopInputFormat) fileFormat.createFileFormatReader(this.entityName.getTabName(), cols, storeConfig, hiveTab);


                startCommonWrite(outputFileFormat, hdfsHelper.fileSystem, storeConfig.getJobConf()
                        , lineReceiver, writerSliceConfig, fileName, this.getTaskPluginCollector());

            } catch (Exception e) {
                throw new RuntimeException("fileName:" + fileName, e);
            }
        }


        public static void startCommonWrite(HadoopInputFormat outputFileFormat, FileSystem fileSystem
                , JobConf conf, RecordReceiver lineReceiver, Configuration config, String fileName, TaskPluginCollector taskPluginCollector) {

            //  RecordWriter recordsWriter = ;

            List<IColMetaGetter> colsMeta = HdfsColMeta.getColsMeta(config);
            // List<Configuration> columns = config.getListConfiguration(Key.COLUMN);
            // String compress = config.getString(Key.COMPRESS, null);
            // List<String> columnNames = colsMeta.stream().map((c) -> c.getName()).collect(Collectors.toList());
            ColumnTypeValInspectors columnTypeInspectors = FileFormatUtils.getColumnTypeInspectors(colsMeta);
            StructObjectInspector inspector = columnTypeInspectors.getStructObjectInspector();
//        ObjectInspectorFactory
//                .getStandardStructObjectInspector(columnNames, columnTypeInspectors.columnTypeInspectors);

            // OrcSerde orcSerde = new OrcSerde();

//            FileOutputFormat outFormat = new OrcOutputFormat();
//            if (!"NONE".equalsIgnoreCase(compress) && null != compress) {
//                Class<? extends CompressionCodec> codecClass = getCompressCodec(compress);
//                if (null != codecClass) {
//                    outFormat.setOutputCompressorClass(conf, codecClass);
//                }
//            }

            //  Object key = outputFileFormat.createKey();
            outputFileFormat.createValue(colsMeta.size());

            FileSinkOperator.RecordWriter writer = null;
            try {
                writer = outputFileFormat.createRecordsWriter(fileSystem, fileName);// outFormat.getRecordWriter(fileSystem, conf, fileName, Reporter.NULL);
                Record record = null;
                Object[] rowVals = new Object[colsMeta.size()];
                while ((record = lineReceiver.getFromReader()) != null) {
                    MutablePair<Object[], Boolean> transportResult
                            = FileFormatUtils.transportOneRecord(record, rowVals, columnTypeInspectors.columnValGetters, taskPluginCollector);
                    if (!transportResult.getRight()) {
                        writer.write(outputFileFormat.getSerde().serialize(transportResult.getLeft(), inspector));
                    }
                }

            } catch (Exception e) {
//                String message = String.format("写文件文件[%s]时发生IO异常,请检查您的网络是否正常！", fileName);
//                HdfsHelper.LOG.error(message);
                Path path = new Path(fileName);
                HdfsHelper.deleteDir(fileSystem, path.getParent());
                throw DataXException.asDataXException(HdfsWriterErrorCode.Write_FILE_IO_ERROR, e);
            } finally {
                try {
                    writer.close(true);
                } catch (Throwable e) {

                }
            }
        }

    }


}
