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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataXGenerateCfgs;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.dump.INameWithPathGetter;
import com.qlangtech.tis.dump.hive.BindHiveTableTool;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.ExecuteResult;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITableBuildTask;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.indexbuild.DftTabPartition;
import com.qlangtech.tis.fullbuild.indexbuild.IPartionableWarehouse;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilder;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilderDescriptor;
import com.qlangtech.tis.fullbuild.taskflow.hive.JoinHiveTask;
import com.qlangtech.tis.hdfs.impl.HdfsPath;
import com.qlangtech.tis.hive.HdfsFileType;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.TabPartitions;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 14:48
 * @see com.qlangtech.tis.plugin.datax.TisDataXHiveWriter
 **/
@Public
public class DataXHiveWriter extends BasicFSWriter
        implements IFlatTableBuilder, IDataSourceFactoryGetter, IDataXBatchPost, IPartionableWarehouse {
    private static final String DATAX_NAME = "Hive";

    @FormField(identity = false, ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;
    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {Validator.require})
    public String partitionFormat;

    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer partitionRetainNum;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    // 目标源中是否自动创建表，这样会方便不少
    public AutoCreateTable autoCreateTable;

    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    @Override
    public String getTemplate() {
        return this.template;
    }

    public MREngine getEngineType() {
        return MREngine.HIVE;
    }

    @Override
    protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap, Optional<RecordTransformerRules> transformerRules) {

        return new HiveDataXContext("tishivewriter", convertTableMapper(tableMap), this.dataXName, transformerRules);
    }

    @Override
    public String appendTabPrefix(String rawTabName) {
        return autoCreateTable.appendTabPrefix(rawTabName);
    }

    /**
     * // PARQUET 不支持date类型 需要将date类型转成timestamp类型
     * <p>
     * Caused by: java.lang.IllegalArgumentException: Unsupported primitive data type: DATE
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.createWriter(DataWritableWriter.java:135)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.access$000(DataWritableWriter.java:58)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter$GroupDataWriter.<init>(DataWritableWriter.java:184)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter$MessageDataWriter.<init>(DataWritableWriter.java:208)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.createMessageWriter(DataWritableWriter.java:93)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.write(DataWritableWriter.java:80)
     * </p>
     *
     * @param tableMap
     * @return
     */
    private TableMap convertTableMapper(TableMap tableMap) {
        return new HiveTableMap(tableMap);
    }

    /**
     * <p>
     * Caused by: java.lang.IllegalArgumentException: Unsupported primitive data type: DATE
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.createWriter(DataWritableWriter.java:135)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.access$000(DataWritableWriter.java:58)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter$GroupDataWriter.<init>(DataWritableWriter.java:184)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter$MessageDataWriter.<init>(DataWritableWriter.java:208)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.createMessageWriter(DataWritableWriter.java:93)
     * at org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter.write(DataWritableWriter.java:80)
     * </p>
     *
     * @see org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter #createWriter
     */
    private class HiveTableMap extends IDataxProcessor.TableMap {
        public HiveTableMap(IDataxProcessor.TableMap tableMap) {
            super(Optional.of(tableMap.getFrom()), tableMap.getSourceCols());
            this.setFrom(tableMap.getFrom());
            this.setTo(tableMap.getTo());
        }

        @Override
        protected List<CMeta> rewriteCols(final List<CMeta> cmetas) {

            return cmetas.stream().map((col) -> {
                DataType type = col.getType();
                if (type.getJdbcType() == JDBCTypes.DECIMAL) {
                    /**
                     * hive的精度不能大于38
                     * Caused by: java.lang.IllegalArgumentException: Decimal precision out of allowed range [1,38]
                     * 	at org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils.validateParameter(HiveDecimalUtils.java:43)
                     * 	at org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo.<init>(DecimalTypeInfo.java:36)
                     * 	at com.alibaba.datax.plugin.writer.hdfswriter.FileFormatUtils$1.decimalType(FileFormatUtils.java:249)
                     */
                    DataType fixType = DataType.create(type.getJdbcType().getType(), type.typeName, Math.min(type.getColumnSize(), 38));
                    fixType.setDecimalDigits(type.getDecimalDigits());
                    col.setType(fixType);
                    return col;
                }

                if (fileType.getType() == HdfsFileType.PARQUET) {
                    if (type.getJdbcType() == JDBCTypes.DATE
                            || type.getJdbcType() == JDBCTypes.TIME) {
                        col.setType(DataType.getType(JDBCTypes.TIMESTAMP));
                    }
                }

                return col;
            }).collect(Collectors.toList());
        }
    }


    @Override
    public TimeFormat getPsFormat() {
        return TimeFormat.parse(this.partitionFormat);
    }

    @Override
    public EntityName parseEntity(ISelectedTab tab) {
        return getDumpTab(this.autoCreateTable.appendTabPrefix(tab.getEntityName().getTabName()));
    }

    /**
     * ========================================================
     * implements: IFlatTableBuilder
     *
     * @see IFlatTableBuilder
     */
    @Override
    public ExecuteResult startTask(ITableBuildTask dumpTask) {

        try {
            try (JDBCConnection conn = getConnection()) {
                HiveDBUtils.executeNoLog(conn, "SET hive.exec.dynamic.partition = true");
                HiveDBUtils.executeNoLog(conn, "SET hive.exec.dynamic.partition.mode = nonstrict");
                return dumpTask.process(new ITaskContext() {
                    @Override
                    public JDBCConnection getObj() {
                        return conn;
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, boolean isFinalNode, IExecChainContext execChainContext, ITaskContext tskContext
            , IJoinTaskStatus joinTaskStatus
            , IDataSourceFactoryGetter dsGetter, Supplier<IPrimaryTabFinder> primaryTabFinder) {
        JoinHiveTask joinHiveTask = new JoinHiveTask(nodeMeta, isFinalNode, primaryTabFinder
                , joinTaskStatus, this.getFs().getFileSystem(), getEngineType(), dsGetter, this);
        //  joinHiveTask.setTaskContext(tskContext);
        joinHiveTask.setContext(execChainContext, tskContext);
        return joinHiveTask;
    }

    /**
     * END implements: IFlatTableBuilder
     * ==================================================================
     */

    @Override
    public boolean isGenerateCreateDDLSwitchOff() {
        return !autoCreateTable.enabled();
    }

    @Override
    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(SourceColMetaGetter sourceColMetaGetter
            , IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {

        return Objects.requireNonNull(autoCreateTable, "autoCreateTable can not be null")
                .createSQLDDLBuilder(this, sourceColMetaGetter, convertTableMapper(tableMapper), transformers).build();
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHiveWriter.class, "DataXHiveWriter-tpl.json");
    }

    public JDBCConnection getConnection() {
        Hiveserver2DataSourceFactory dsFactory = getDataSourceFactory();
        String jdbcUrl = dsFactory.getJdbcUrl();
        try {
            return dsFactory.getConnection((jdbcUrl), Optional.empty(), false);
        } catch (SQLException e) {
            throw new RuntimeException(jdbcUrl, e);
        }
    }

    @Override
    protected void startScanFSWriterDependency() {
        this.getHiveConnGetter();
    }
    
    @Override
    public Hiveserver2DataSourceFactory getDataSourceFactory() {
        if (StringUtils.isBlank(this.dbName)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return BasicDataXRdbmsWriter.getDs(this.dbName);
    }

    @Override
    public Integer getRowFetchSize() {
        throw new UnsupportedOperationException();
    }

    public final IHiveConnGetter getHiveConnGetter() {
        return this.getDataSourceFactory();
    }


    public class HiveDataXContext extends FSDataXContext {

        private final String dataxPluginName;

        public HiveDataXContext(String dataxPluginName, IDataxProcessor.TableMap tabMap, String dataXName, Optional<RecordTransformerRules> transformerRules) {
            super(tabMap, dataXName, transformerRules);
            this.dataxPluginName = dataxPluginName;
        }

        @Override
        public final String getTableName() {
            final String tabName = super.getTableName();
            return autoCreateTable.appendTabPrefix(tabName);
//            Optional<String> tabPrefix = autoCreateTable.getMapperTabPrefix();
//            return tabPrefix.map((prefix) -> (prefix + tabName)).orElse(tabName);
            //  return super.getTableName();// tabDecorator.decorate();
        }

        public String getDataxPluginName() {
            return this.dataxPluginName;
        }

        public Integer getPartitionRetainNum() {
            return partitionRetainNum;
        }

        public String getPartitionFormat() {
            return partitionFormat;
        }
    }


    /**
     * ========================================================
     * impl:
     *
     * @see IDataXBatchPost
     */
    @Override
    public ExecutePhaseRange getPhaseRange() {
        return new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN);
    }

    @Override
    public IRemoteTaskPreviousTrigger createPreExecuteTask(IExecChainContext execContext, EntityName dumpTable, ISelectedTab tab) {
        // final EntityName dumpTable = getDumpTab(tab);
        Objects.requireNonNull(partitionRetainNum, "partitionRetainNum can not be null");
        return new IRemoteTaskPreviousTrigger() {
            @Override
            public String getTaskName() {
                return IDataXBatchPost.getPreExecuteTaskName(dumpTable);
            }

            @Override
            public void run() {

                // 负责初始化表
                final Hiveserver2DataSourceFactory dsFactory = DataXHiveWriter.this.getDataSourceFactory();
                INameWithPathGetter tabPath = dsFactory.getSubTablePath(dumpTable);// INameWithPathGetter.create(Optional.of(dsFactory.getAlternativeHdfsSubPath()), dumpTable.getTabName());
                ITISFileSystem fs = getFs().getFileSystem();
                Path tabDumpParentPath = getTabDumpParentPath(execContext, tabPath);// new Path(fs.getRootDir().unwrap(Path.class), getHdfsSubPath(dumpTable));
                dsFactory.visitFirstConnection((conn) -> {
                    try {
                        Objects.requireNonNull(tabDumpParentPath, "tabDumpParentPath can not be null");
                        // Hiveserver2DataSourceFactory dsFactory = getDataSourceFactory();
                        final IPath parentPath = fs.getPath(new HdfsPath(tabDumpParentPath), "..");
                        JoinHiveTask.initializeTable(dsFactory
                                , conn, dumpTable,
                                new JoinHiveTask.IHistoryTableProcessor() {
                                    @Override
                                    public void cleanHistoryTable() throws IOException {
                                        JoinHiveTask.cleanHistoryTable(fs, parentPath, dsFactory, conn, dumpTable, partitionRetainNum);
                                    }
                                }
                                , () -> true //
                                , () -> {
                                    // 建新表
                                    try {
                                        BasicDataXRdbmsWriter.process(dataXName, execContext.getProcessor()
                                                , DataXHiveWriter.this, DataXHiveWriter.this, conn, dumpTable, tab.getEntityName().getTabName());
                                        // 需要将缓存失效
                                        dsFactory.refresh();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        };
    }

    @Override
    public IRemoteTaskPostTrigger createPostTask(
            IExecChainContext execContext, EntityName entity, ISelectedTab tab, IDataXGenerateCfgs cfgFileNames) {

        return new IRemoteTaskPostTrigger() {

            @Override
            public String getTaskName() {
                return getEngineType().getToken() + "_" + entity.getTabName() + "_bind";
            }

            @Override
            public void run() {
                bindHiveTables(execContext, entity);
            }
        };
    }


    public EntityName getDumpTab(String tabName) {
        return EntityName.create(this.getDataSourceFactory().getDbName(), tabName);
    }

    private String getHdfsSubPath(IExecChainContext execContext, INameWithPathGetter dumpTable) {
        Objects.requireNonNull(dumpTable, "dumpTable can not be null");
        // return dumpTable.getNameWithPath() + "/" + DataxUtils.getDumpTimeStamp();
        return dumpTable.getNameWithPath() + "/"
                + this.getPsFormat().format(execContext.getPartitionTimestampWithMillis());
    }

    private Path getTabDumpParentPath(IExecChainContext execContext, INameWithPathGetter dumpTable) {
        // EntityName dumpTable = getDumpTab(tab);
        ITISFileSystem fs = getFs().getFileSystem();
        Path tabDumpParentPath = new Path(fs.getRootDir().unwrap(Path.class), getHdfsSubPath(execContext, dumpTable));
        return tabDumpParentPath;
    }


    private void bindHiveTables(IExecChainContext execContext, EntityName dumpTable) {
        try {

            // EntityName dumpTable = getDumpTab(tab);
            String dumpTimeStamp = TimeFormat.parse(this.partitionFormat).format(execContext.getPartitionTimestampWithMillis());

            if (!execContext.isDryRun()) {
                Hiveserver2DataSourceFactory dsFactory = this.getDataSourceFactory();
                INameWithPathGetter tabPath = dsFactory.getSubTablePath(dumpTable); // INameWithPathGetter.create(Optional.of(dsFactory.getAlternativeHdfsSubPath()), dumpTable.getTabName());
                Path tabDumpParentPath = getTabDumpParentPath(execContext, tabPath);

                try (JDBCConnection hiveConn = this.getConnection()) {
                    final Path dumpParentPath = tabDumpParentPath;
                    BindHiveTableTool.bindHiveTables(dsFactory, hiveConn, this.getFs().getFileSystem()
                            , Collections.singletonMap(dumpTable, () -> {
                                        return new BindHiveTableTool.HiveBindConfig(Collections.emptyList(), dumpParentPath);
                                    }
                            )
                            , dumpTimeStamp
                            , (columns, hiveTable) -> {
                                return true;
                            },
                            (columns, hiveTable) -> {
                                throw new UnsupportedOperationException("table " + hiveTable.getTabName() + " shall have create in 'createPreExecuteTask'");
                            }
                    );

                }
            }

            recordPt(execContext, dumpTable, dumpTimeStamp);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 添加当前任务的pt
     */
    private void recordPt(IExecChainContext execContext, EntityName dumpTable, String dumpTimeStamp) {
        if (StringUtils.isEmpty(dumpTimeStamp)) {
            throw new IllegalArgumentException("param dumpTimeStamp can not be empty");
        }
        TabPartitions dateParams = ExecChainContextUtils.getDependencyTablesPartitions(execContext);
        dateParams.putPt(dumpTable, new DftTabPartition(dumpTimeStamp));
    }


    /**
     * impl End: IDataXBatchPost
     * ========================================================
     */
    @TISExtension()
    public static class DefaultDescriptor extends HdfsWriterDescriptor implements IFlatTableBuilderDescriptor {
        public DefaultDescriptor() {
            super();
            //this.registerSelectOptions(KEY_FIELD_NAME_HIVE_CONN, () -> ParamsConfig.getItems(IHiveConnGetter.PLUGIN_NAME));
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        public boolean validatePartitionRetainNum(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer retainNum = Integer.parseInt(value);
            if (retainNum < 1 || retainNum > 5) {
                msgHandler.addFieldError(context, fieldName, "数目必须为不小于1且不大于5之间");
                return false;
            }
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.HiveMetaStore;
        }
//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            return HiveFlatTableBuilder.validateHiveAvailable(msgHandler, context, postFormVals);
//        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
