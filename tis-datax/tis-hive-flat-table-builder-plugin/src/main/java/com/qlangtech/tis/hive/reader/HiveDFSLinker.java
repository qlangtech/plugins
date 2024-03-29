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
import com.alibaba.datax.plugin.unstructuredstorage.Compress;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.datax.Delimiter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import com.qlangtech.tis.plugin.datax.format.TextFormat;
import com.qlangtech.tis.plugin.tdfs.IExclusiveTDFSType;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.utils.DBsGetter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * @param entityName
     * @return
     * @see org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
     */
    public FileFormat getFileFormat(String entityName) {
        if (StringUtils.isEmpty(entityName)) {
            throw new IllegalArgumentException("param entityName can not be empty");
        }

        Hiveserver2DataSourceFactory dfFactory = getDataSourceFactory();

        HiveTable table = dfFactory.metadata.createMetaStoreClient().getTable(dfFactory.dbName, entityName);
        HiveTable.StoredAs storedAs = table.getStoredAs();
        SerDeInfo sdInfo = storedAs.getSerdeInfo();
        Map<String, String> sdParams = sdInfo.getParameters();
        try {
            Class<?> outputFormatClass = Class.forName(storedAs.outputFormat, false, HiveDFSLinker.class.getClassLoader());
            if (org.apache.hadoop.mapred.TextOutputFormat.class.isAssignableFrom(outputFormatClass)) {


                TextFormat txtFormat = new TextFormat();
                txtFormat.header = false;

                txtFormat.fieldDelimiter = Delimiter.parseByVal(sdParams.get(serdeConstants.FIELD_DELIM)).token;
                txtFormat.nullFormat = sdParams.get(serdeConstants.SERIALIZATION_NULL_FORMAT);
                txtFormat.dateFormat = "yyyy-MM-dd";
                txtFormat.compress = Compress.none.token;
                txtFormat.encoding = TisUTF8.getName();
                return txtFormat;
            } else {
                throw new IllegalStateException("outputFormatClass:" + outputFormatClass.getName() + " can not be resolved");
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
