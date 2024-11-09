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

package com.qlangtech.tis.hive.reader;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.HiveTable.HiveTabColType;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.hive.reader.impl.HadoopParquetInputFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopTextInputFormat;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.TableColsMeta;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.tdfs.IExclusiveTDFSType;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.utils.DBsGetter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-19 15:43
 **/
public class HiveDFSLinker extends TDFSLinker {
    public final static String NAME_DESC = "Hive";

    @FormField(ordinal = 5, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String fsName;
    public transient FileSystemFactory fileSystem;

    @Override
    public String getRootPath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITDFSSession createTdfsSession(Integer timeout) {
        return createTdfsSession();
    }

    @Override
    public ITDFSSession createTdfsSession() {
        return new HiveDFSSession("rootpath", () -> getFs(), this);
    }

    public FileSystemFactory getFs() {
        if (fileSystem == null) {
            this.fileSystem = FileSystemFactory.getFsFactory(fsName);
        }
        Objects.requireNonNull(this.fileSystem, "fileSystem has not be initialized");
        return fileSystem;
    }

    public TableColsMeta getTabsMeta() {
        Hiveserver2DataSourceFactory dsFactory = getDataSourceFactory();
        return new TableColsMeta(dsFactory, dsFactory.dbName);
    }


    public Hiveserver2DataSourceFactory getDataSourceFactory() {
        if (StringUtils.isBlank(this.linker)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return BasicDataXRdbmsWriter.getDs(this.linker);
    }

    @Override
    public <T> T useTdfsSession(TDFSSessionVisitor<T> tdfsSession) {
        try {
            return tdfsSession.accept(createTdfsSession());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

//    static final TextFormat txtFormat;
//
//    static {
//        txtFormat = new TextFormat();
//        txtFormat.header = false;
//        txtFormat.fieldDelimiter = Delimiter.Tab.token;
//        txtFormat.compress = Compress.none.token;
//        txtFormat.encoding = "utf-8";
//    }

    /**
     * 获取读hive对应 HDFS 文件 Reader 器
     *
     * @param entityName
     * @return
     * @see org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
     * @see org.apache.hadoop.mapred.TextInputFormat
     */
    public FileFormat getInputFileFormat(String entityName) {
        if (StringUtils.isEmpty(entityName)) {
            throw new IllegalArgumentException("param entityName can not be empty");
        }

        Hiveserver2DataSourceFactory dfFactory = getDataSourceFactory();
        org.apache.hadoop.conf.Configuration conf = getFs().getConfiguration();
        HiveTable table = dfFactory.metadata.createMetaStoreClient().getTable(dfFactory.dbName, entityName);
        HiveTable.StoredAs storedAs = table.getStoredAs();
        SerDeInfo sdInfo = storedAs.getSerdeInfo();
        Map<String, String> sdParams = sdInfo.getParameters();
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : sdParams.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }
        try {
            // example: MapredParquetInputFormat for Parquet
            Class<?> inputFormatClass = Class.forName(storedAs.inputFormat, false, HiveDFSLinker.class.getClassLoader());
            // forExample : LazySimpleSerDe, ParquetHiveSerDe
            AbstractSerDe serde = (AbstractSerDe) Class.forName(
                    sdInfo.getSerializationLib()
                    , false, HiveDFSLinker.class.getClassLoader()).getDeclaredConstructor().newInstance();

            // String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);

            List<HiveTabColType> cols = table.getCols();
            props.setProperty(serdeConstants.LIST_COLUMNS
                    , cols.stream().map((col) -> col.getColName()).collect(Collectors.joining(String.valueOf(SerDeUtils.COMMA))));
            props.setProperty(serdeConstants.LIST_COLUMN_TYPES
                    , cols.stream().map((col) -> col.getType()).collect(Collectors.joining(String.valueOf(SerDeUtils.COMMA))));
            JobConf jobConf = new JobConf(conf);
            serde.initialize(jobConf, props);

            if (org.apache.hadoop.mapred.TextInputFormat.class.isAssignableFrom(inputFormatClass)) {
                org.apache.hadoop.mapred.TextInputFormat inputFormat
                        = (org.apache.hadoop.mapred.TextInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
                inputFormat.configure(jobConf);
                return new HadoopTextInputFormat(entityName, cols.size(), inputFormat, serde, jobConf);
            } else if (MapredParquetInputFormat.class == inputFormatClass) {
                MapredParquetInputFormat pqInputFormat = (MapredParquetInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
                return new HadoopParquetInputFormat(entityName, cols.size(), pqInputFormat, serde, jobConf);
            } else {
                throw new IllegalStateException("outputFormatClass:" + inputFormatClass.getName() + " can not be resolved");
            }
            // HiveIgnoreKey
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        //  storedAs.inputFormat;

//        Map<String, String> params = sdInfo.getParameters();
//        try {
//            if (Class.forName(sdInfo.getSerializationLib()) == LazySimpleSerDe.class) {
//                params.get(serdeConstants.FIELD_DELIM);
//            }
//        } catch (ClassNotFoundException e) {
//            throw new RuntimeException(e);
//        }
        // return txtFormat;
    }

//    private class HdfsTextReader extends TextFormat {
//        private final byte[] lineDelimite;
//
//        public HdfsTextReader(byte[] lineDelimite) {
//            this.lineDelimite = lineDelimite;
//        }
//
//        @Override
//        public UnstructuredReader createReader(InputStream input) {
//
//            org.apache.hadoop.util.LineReader lineReader //
//                    = new org.apache.hadoop.util.LineReader(input, lineDelimite);
//            Text line = new Text();
//            return new UnstructuredReader() {
//                @Override
//                public boolean hasNext() throws IOException {
//                    line.clear();
//                    return lineReader.readLine(line) > 0;
//                }
//
//                @Override
//                public String[] next() throws IOException {
//
//                    // line.
//                    line.write();
//                    return new String[59];
//                }
//
//                @Override
//                public String[] getHeader() {
//                    throw new UnsupportedOperationException();
//                }
//
//                @Override
//                public void close() throws IOException {
//                    IOUtils.close(input);
//                }
//            };
//
//
//        }
//    }


    @TISExtension
    public static final class DftDescriptor extends BasicDescriptor implements IExclusiveTDFSType {

        public DftDescriptor() {
            super();
            this.registerSelectOptions(ITISFileSystemFactory.KEY_FIELD_NAME_FS_NAME
                    , () -> TIS.getPluginStore(FileSystemFactory.class)
                            .getPlugins().stream()
                            .filter(((f) -> f instanceof HdfsFileSystemFactory)).collect(Collectors.toList()));
        }

        @Override
        public IEndTypeGetter.EndType getTDFSType() {
            return IEndTypeGetter.EndType.HiveMetaStore;
        }

        @Override
        public String getDisplayName() {
            return NAME_DESC;
        }

        @Override
        protected List<? extends IdentityName> createRefLinkers() {
            return DBsGetter.getInstance().getExistDbs(Hiveserver2DataSourceFactory.NAME_HIVESERVER2);
        }

        @Override
        public boolean validateLinker(IFieldErrorHandler msgHandler, Context context, String fieldName, String linker) {
            return true;
        }
    }
}
