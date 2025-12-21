

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataXGenerateCfgs;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TabCols;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.ExecuteResult;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
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
import com.qlangtech.tis.config.aliyun.IAliyunAccessKey;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.odps.JoinOdpsTask;
import com.qlangtech.tis.plugin.datax.odps.OdpsDataSourceFactory;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.TabPartitions;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXOdpsWriter extends BasicDataXRdbmsWriter implements IFlatTableBuilder, IDataSourceFactoryGetter, IDataXBatchPost, IPartionableWarehouse {
    private static final String DATAX_NAME = "Aliyun-ODPS";
    private static final Logger logger = LoggerFactory.getLogger(DataXOdpsWriter.class);

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean truncate;

    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer lifecycle;

    @FormField(ordinal = 8, type = FormFieldType.ENUM, validate = {Validator.require})
    public String partitionFormat;

    @Override
    public String appendTabPrefix(String rawTabName) {
        return autoCreateTable.appendTabPrefix(rawTabName);
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXOdpsWriter.class, "DataXOdpsWriter-tpl.json");
    }

    @Override
    public ExecutePhaseRange getPhaseRange() {
        return new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN);
    }

    @Override
    public EntityName parseEntity(ISelectedTab tab) {
        // return EntityName.parse(ta);
        return EntityName.create(this.getDataSourceFactory().getDbConfig().getName()
                , this.autoCreateTable.appendTabPrefix(tab.getEntityName().getTabName()));
    }

    @Override
    public IRemoteTaskPreviousTrigger createPreExecuteTask(IExecChainContext execContext, EntityName entity, ISelectedTab tab) {
        return new IRemoteTaskPreviousTrigger() {
            @Override
            public String getTaskName() {
                return IDataXBatchPost.getPreExecuteTaskName(entity);
            }

            @Override
            public void run() {

                // 负责初始化表
                OdpsDataSourceFactory ds = DataXOdpsWriter.this.getDataSourceFactory();
                //  EntityName dumpTable = getDumpTab(tab);
                // ITISFileSystem fs = getFs().getFileSystem();
                // Path tabDumpParentPath = getTabDumpParentPath(tab);// new Path(fs.getRootDir().unwrap(Path.class), getHdfsSubPath(dumpTable));
                ds.visitFirstConnection((conn) -> {
                    try {
//                        Objects.requireNonNull(tabDumpParentPath, "tabDumpParentPath can not be null");
//                        JoinHiveTask.initializeHiveTable(fs
//                                , fs.getPath(new HdfsPath(tabDumpParentPath), "..")
//                                , getDataSourceFactory()
//                                , conn.getConnection(), dumpTable, partitionRetainNum, () -> true, () -> {
//                                    try {
//                                        BasicDataXRdbmsWriter.process(dataXName, execContext.getProcessor(), DataXOdpsWriter.this
//                                                , DataXOdpsWriter.this, conn, tab.getName());
//                                    } catch (Exception e) {
//                                        throw new RuntimeException(e);
//                                    }
//                                });

                        try {
                            BasicDataXRdbmsWriter.process(dataXName, execContext.getProcessor(), DataXOdpsWriter.this
                                    , DataXOdpsWriter.this, conn, entity, tab.getEntityName().getTabName());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        };
    }

//    @Override
//    public EntityName parseEntity(ISelectedTab tab) {
//        return getDumpTab(tab.getName());
//    }

//    private EntityName getDumpTab(String tabName) {
//        return EntityName.create(this.getDataSourceFactory().getDbName(), tabName);
//    }


    @Override
    public OdpsDataSourceFactory getDataSourceFactory() {
        return (OdpsDataSourceFactory) super.getDataSourceFactory();
    }

    @Override
    public IRemoteTaskPostTrigger createPostTask(
            IExecChainContext execContext, EntityName entity, ISelectedTab tab, IDataXGenerateCfgs cfgFileNames) {

        return new IRemoteTaskPostTrigger() {
            @Override
            public String getTaskName() {
                return "odps_" + entity.getTabName() + "_bind";
            }

            @Override
            public void run() {
                bindHiveTables(execContext, entity);
            }
        };
    }

    private void bindHiveTables(IExecChainContext execContext, EntityName tab) {
        String dumpTimeStamp = this.getPsFormat().format(execContext.getPartitionTimestampWithMillis());
        recordPt(execContext, tab, dumpTimeStamp);
    }


    /**
     * 添加当前任务的pt
     */
    private void recordPt(IExecChainContext execContext, EntityName dumpTable, String dumpTimeStamp) {
        //  Map<IDumpTable, ITabPartition> dateParams = ExecChainContextUtils.getDependencyTablesPartitions(execChainContext);
        if (StringUtils.isEmpty(dumpTimeStamp)) {
            throw new IllegalArgumentException("param dumpTimeStamp can not be empty");
        }
        TabPartitions dateParams = ExecChainContextUtils.getDependencyTablesPartitions(execContext);
        dateParams.putPt(dumpTable, new DftTabPartition(dumpTimeStamp));
    }


    @Override
    public DataflowTask createTask(ISqlTask nodeMeta, boolean isFinalNode, IExecChainContext tplContext
            , ITaskContext tskContext, IJoinTaskStatus joinTaskStatus, IDataSourceFactoryGetter dsGetter
            , Supplier<IPrimaryTabFinder> primaryTabFinder) {

        JoinOdpsTask odpsTask
                = new JoinOdpsTask(this, dsGetter, nodeMeta, isFinalNode, primaryTabFinder, joinTaskStatus, this);
        odpsTask.setContext(tplContext, tskContext);
        return odpsTask;
    }


    @Override
    public ExecuteResult startTask(ITableBuildTask dumpTask) {

        try (JDBCConnection conn = this.getConnection()) {
            return dumpTask.process(new ITaskContext() {
                @Override
                public JDBCConnection getObj() {
                    return conn;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public JDBCConnection getConnection() {
        OdpsDataSourceFactory dsFactory = getDataSourceFactory();
        String jdbcUrl = dsFactory.getJdbcUrl();
        try {
            return dsFactory.getConnection(jdbcUrl, Optional.empty(), false);
        } catch (SQLException e) {
            throw new RuntimeException(String.valueOf(jdbcUrl), e);
        }
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }

        return new OdpsContext(this, tableMap.get(), transformerRules);
    }

    @Override
    public Class<?> getOwnerClass() {
        return DataXOdpsWriter.class;
    }

    @Override
    public TimeFormat getPsFormat() {
        return TimeFormat.parse(this.partitionFormat);
    }


    public static class OdpsContext implements IDataxContext {
        private final DataXOdpsWriter odpsWriter;
        private final IDataxProcessor.TableMap tableMapper;
        private final IAliyunAccessKey accessKey;
        private final OdpsDataSourceFactory dsFactory;
        private final List<String> cols;

        public OdpsContext(DataXOdpsWriter odpsWriter, IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformerRules) {
            this.odpsWriter = odpsWriter;
            this.tableMapper = tableMapper;
            this.cols = TabCols.create(null, tableMapper, transformerRules).getRawCols();
            this.dsFactory = odpsWriter.getDataSourceFactory();
            this.accessKey = this.dsFactory.getAccessKey();

        }

        public String getTable() {
            return this.tableMapper.createTargetTableName(odpsWriter.autoCreateTable);
        }

        public String getProject() {
            return this.dsFactory.project;
        }

        public List<String> getColumn() {
//            return this.tableMapper.getSourceCols()
//                    .stream().map((col) -> col.getName()).collect(Collectors.toList());
            return this.cols;
        }

        public String getAccessId() {
            return this.accessKey.getAccessKeyId();
        }

        public String getAccessKey() {
            return this.accessKey.getAccessKeySecret();
        }

        public boolean isTruncate() {
            return this.odpsWriter.truncate;
        }

        public String getOdpsServer() {
            return this.dsFactory.odpsServer;
        }

        public String getTunnelServer() {
            return this.dsFactory.tunnelServer;
        }

        public String getPartitionTimeFormat() {
            return odpsWriter.partitionFormat;
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor
            implements IFlatTableBuilderDescriptor {
        public DefaultDescriptor() {
            super();
        }


        public boolean validatePartitionFormat(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {

            try {
                TimeFormat.parse(val);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }

            return true;
        }

        /**
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param val
         * @return
         */
        public boolean validateLifecycle(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int lifecycle = Integer.parseInt(val);
            final int MIN_Lifecycle = 3;
            if (lifecycle < MIN_Lifecycle) {
                msgHandler.addFieldError(context, fieldName, "不能小于" + MIN_Lifecycle + "天");
                return false;
            }
            return true;
        }


        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.AliyunODPS;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
