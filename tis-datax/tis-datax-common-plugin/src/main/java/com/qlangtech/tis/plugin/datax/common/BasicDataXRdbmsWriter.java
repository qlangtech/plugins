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

package com.qlangtech.tis.plugin.datax.common;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.rdbms.writer.util.SelectTable;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.datax.DataXCfgFile;
import com.qlangtech.tis.datax.IDataXBatchPost;
import com.qlangtech.tis.datax.IDataXGenerateCfgs;
import com.qlangtech.tis.datax.IDataXNameAware;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPostTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteTaskPreviousTrigger;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.AbstractCreateTableSqlBuilder.CreateDDL;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-23 12:07
 **/
public abstract class BasicDataXRdbmsWriter<DS extends DataSourceFactory> extends DataxWriter
        implements IDataSourceFactoryGetter, IInitWriterTableExecutor, KeyedPluginStore.IPluginKeyAware, IDataXNameAware, IDataXBatchPost {
    public static final String KEY_DB_NAME_FIELD_NAME = "dbName";
    private static String TABLE_NAME_PLACEHOLDER = "@table";
    private static final Logger logger = LoggerFactory.getLogger(BasicDataXRdbmsWriter.class);

    @FormField(identity = false, ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {})
    public String preSql;

    @FormField(ordinal = 6, type = FormFieldType.TEXTAREA, validate = {})
    public String postSql;

    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, validate = {})
    public String session;

    @FormField(ordinal = 12, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer batchSize;

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    // 目标源中是否自动创建表，这样会方便不少
    public AutoCreateTable autoCreateTable;

    @Override
    public void startScanDependency() {
        this.getDataSourceFactory();
    }

    @Override
    public ExecutePhaseRange getPhaseRange() {
        return new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.FullDump);
    }

    @Override
    public IRemoteTaskPreviousTrigger createPreExecuteTask(IExecChainContext execContext, ISelectedTab tab) {
        if (StringUtils.isBlank(this.preSql)) {
            return null;
        }
        return new PreAndPostSQLExecutor(true, execContext, tab);
    }

    @Override
    public IRemoteTaskPostTrigger createPostTask(IExecChainContext execContext, ISelectedTab tab, IDataXGenerateCfgs cfgFileNames) {
        if (StringUtils.isBlank(this.postSql)) {
            return null;
        }
        return new PreAndPostSQLExecutor(false, execContext, tab);
    }


    private class PreAndPostSQLExecutor implements IRemoteTaskPostTrigger, IRemoteTaskPreviousTrigger {
        private final boolean preExecute;
        private final IExecChainContext execContext;
        private final ISelectedTab tab;

        public PreAndPostSQLExecutor(boolean preExecute, IExecChainContext execContext, ISelectedTab tab) {
            this.preExecute = preExecute;
            this.execContext = execContext;
            this.tab = tab;
        }

        @Override
        public String getTaskName() {
            return "execute_" + (this.preExecute ? "pre" : "post") + "SQL_of_" + tab.getName();// IDataXBatchPost.getPreExecuteTaskName(tab);
        }

        private String validateSQL(String sql) {
            if (StringUtils.isBlank(sql)) {
                throw new IllegalStateException(this.getTaskName() + " relevant SQL can not be empty");
            }
            return sql;
        }

        @Override
        public void run() {

            final TableAliasMapper tableAliasMapper
                    = execContext.getAttribute(TableAlias.class.getSimpleName(), () -> {
                return execContext.getProcessor().getTabAlias(null);
            });

            BasicDataSourceFactory dsFactory = ((BasicDataSourceFactory) getDataSourceFactory());
            dsFactory.visitAllConnection((conn) -> {
                SelectTable toTable = SelectTable.create(tableAliasMapper.get(tab).getTo(), dsFactory);
                String preSqlStatement = StringUtils.replace(validateSQL(this.preExecute ? preSql : postSql), TABLE_NAME_PLACEHOLDER, toTable.getTabName());
                try {
                    conn.execute(preSqlStatement);
                    logger.info("success " + getTaskName() + ":" + preSqlStatement);
                } catch (Exception e) {
                    throw new RuntimeException("execute " + getTaskName() + ":" + preSqlStatement, e);
                }
            });
        }
    }


    @Override
    public final CreateDDL generateCreateDDL(
            SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper
            , Optional<RecordTransformerRules> transformers) {
        CreateTableSqlBuilder sqlDDLBuilder
                = Objects.requireNonNull(this.autoCreateTable, "autoCreateTable can not be null")
                .createSQLDDLBuilder(
                        this, sourceColMetaGetter, tableMapper, transformers);
        return sqlDDLBuilder.build();
    }

    /**
     * 是否已经关闭
     *
     * @return
     */
    @Override
    public boolean isGenerateCreateDDLSwitchOff() {
        return !Objects.requireNonNull(autoCreateTable, "autoCreateTable can not be null")
                .enabled();
    }

    @FormField(ordinal = 15, type = FormFieldType.TEXTAREA, advance = false, validate = {Validator.require})
    public String template;

    public transient String dataXName;

    @Override
    public final String getCollectionName() {
        return this.dataXName;
    }

    @Override
    public Integer getRowFetchSize() {
        throw new UnsupportedOperationException("just support in DataX Reader");
    }

    @Override
    public final void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public final String getTemplate() {
        return this.template;
    }

    @Override
    public DS getDataSourceFactory() {
        if (StringUtils.isBlank(this.dbName)) {
            throw new IllegalStateException("prop dbName can not be null");
        }
        return getDs(this.dbName);
    }

    public static <DS extends DataSourceFactory> DS getDs(String dbName) {
        return TIS.getDataBasePlugin(PostedDSProp.parse((dbName)));
        // return (DS) dsStore.getPlugin();
    }


    @Override
    protected Class<RdbmsWriterDescriptor> getExpectDescClass() {
        return RdbmsWriterDescriptor.class;
    }

    @Override
    public final void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
//        if (RobustReflectionConverter2.usedPluginInfo.get().isDryRun()) {
//            return;
//        }
        process(this.dataXName, (BasicDataXRdbmsWriter<BasicDataSourceFactory>) this, targetTabName, jdbcUrls);
    }

    /**
     * 初始化表RDBMS的表，如果表不存在就创建表
     *
     * @param
     * @throws Exception
     */
    private static void process(String dataXName, BasicDataXRdbmsWriter<BasicDataSourceFactory> dataXWriter
            , String tableName, List<String> jdbcUrls) throws Exception {
        IDataxProcessor processor = DataxProcessor.load(null, StoreResourceType.DataApp, dataXName);
        DataSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
        for (String jdbcUrl : jdbcUrls) {
            try (JDBCConnection conn = dsFactory.getConnection((jdbcUrl), false)) {
                process(dataXName, processor, dataXWriter, dataXWriter, conn, tableName);
            }
        }
    }

    /**
     * @param dataXName
     * @param processor
     * @param dsGetter
     * @param dataXWriter
     * @param jdbcConn
     * @param tableName
     * @return tableExist 表是否存在
     */
    public static void process(String dataXName, IDataxProcessor processor
            , IDataSourceFactoryGetter dsGetter, IDataxWriter dataXWriter, JDBCConnection jdbcConn
            , String tableName) {
        if (StringUtils.isEmpty(dataXName)) {
            throw new IllegalArgumentException("param dataXName can not be null");
        }
        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null,dataXName:" + dataXName);
        boolean autoCreateTable = !dataXWriter.isGenerateCreateDDLSwitchOff();
        //  try {
        if (autoCreateTable) {

            jdbcConn.initializeSinkTab(tableName, () -> {
                try {
                    File createDDL = new File(processor.getDataxCreateDDLDir(null)
                            , tableName + DataXCfgFile.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
                    if (!createDDL.exists()) {
                        throw new IllegalStateException("create table script is not exist:" + createDDL.getAbsolutePath());
                    }
                    Connection conn = jdbcConn.getConnection();
                    DataSourceFactory dsFactory = dsGetter.getDataSourceFactory();
                    String createScript = FileUtils.readFileToString(createDDL, TisUTF8.get());
                    final EntityName tab = EntityName.parse(tableName);

                    boolean tableExist = false;
                    List<ColumnMetaData> cols = Lists.newArrayList();
                    try {
                        cols = dsFactory.getTableMetadata(jdbcConn, true, tab);
                        tableExist = true;
                    } catch (TableNotFoundException e) {
                        logger.warn(e.toString());
                    }

                    if (!tableExist) {
                        // 表不存在
                        boolean success = false;
                        String currentExecSql = null;
                        try {
                            List<String> statements = parseStatements(createScript); // Lists.newArrayList(StringUtils.split(createScript, ";"));
                            try (Statement statement = conn.createStatement()) {
                                logger.info("create table:{}\n   script:{}", tab.getFullName(), createScript);
                                for (String execSql : statements) {
                                    currentExecSql = execSql;
                                    success = statement.execute(execSql);
                                }

                            }
                        } catch (SQLException e) {
                            throw new RuntimeException("currentExecSql:" + currentExecSql, e);
                        }
                    } else {
                        logger.info("table:{},cols:{} already exist ,skip the create table step", tab.getFullName()
                                , cols.stream().map((col) -> col.getName()).collect(Collectors.joining(",")));
                    }
                    // return tableExist;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }


            });

        }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        //  return false;
    }

    /**
     * <pre>
     * CREATE TABLE "orderdetail"
     * (
     *     "order_id"             VARCHAR2(60 CHAR),
     *     "global_code"          VARCHAR2(26 CHAR)
     *  , CONSTRAINT orderdetail_pk PRIMARY KEY ("order_id")
     * );
     * COMMENT ON COLUMN "orderdetail"."global_code" IS '1,正常开单;2预订开单;3.排队开单;4.外卖开单';
     * </pre>
     *
     * @param createScript
     * @return
     */
    static List<String> parseStatements(String createScript) {
        List<String> result = Lists.newArrayList();
        StringBuffer sqlStatement = null;
        char c;
        int singleQuotesMetaCount = 0;
        for (int idx = 0; idx < createScript.length(); idx++) {
            c = createScript.charAt(idx);
            if (sqlStatement == null) {
                if (c == '\n' || c == ' ') {
                    continue;
                }
                sqlStatement = new StringBuffer();
            }
            if (c == '\'') {
                singleQuotesMetaCount++;
            }
            if (c != ';' || ((singleQuotesMetaCount & 1) > 0 /** 说明是奇数即在引号内部的';'*/)) {
                sqlStatement.append(c);
            } else {
                result.add(sqlStatement.toString());
                sqlStatement = null;
            }
        }

        if (sqlStatement != null) {
            result.add(sqlStatement.toString());
        }

        return result;
    }


    protected static abstract class RdbmsWriterDescriptor extends BaseDataxWriterDescriptor {
        @Override
        public final boolean isRdbms() {
            return true;
        }

        /**
         * 是否支持自动创建
         *
         * @return
         */
        public boolean isSupportTabCreate() {
            return !this.isRdbms();
        }

        /**
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param val
         * @return
         */
        public boolean validateBatchSize(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int batchSize = Integer.parseInt(val);
            if (batchSize < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于0");
                return false;
            }
            int maxVal = getMaxBatchSize();
            if (batchSize > maxVal) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + maxVal);
                return false;
            }
            return true;
        }

        protected int getMaxBatchSize() {
            return 2024;
        }

        public boolean validateDbName(IFieldErrorHandler msgHandler, Context context, String fieldName, String dbName) {
            BasicDataSourceFactory ds = getDs(dbName);
            if (ds.getJdbcUrls().size() > 1) {
                msgHandler.addFieldError(context, fieldName, "不支持分库数据源，目前无法指定数据路由规则，请选择单库数据源");
                return false;
            }
            return true;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return validateSourceAndTargetEndIsSame(msgHandler, context, postFormVals.newInstance());
        }

        /**
         * 校验源端和目标端不能为同一个相同的数据源，犯这样错误的同学太不应该了
         *
         * @param msgHandler
         * @param context
         * @param targetDataxWriter
         * @return
         */
        private boolean validateSourceAndTargetEndIsSame(IControlMsgHandler msgHandler, Context context, BasicDataXRdbmsWriter targetDataxWriter) {
            IDataxProcessor dataXProcessor = DataxProcessor.load((IPluginContext) msgHandler, msgHandler.getCollectionName());
            IDataxReader dataXRreader = dataXProcessor.getReader((IPluginContext) msgHandler);

            if (dataXRreader instanceof IDataSourceFactoryGetter) {
                if (StringUtils.equals(targetDataxWriter.dbName, ((IDataSourceFactoryGetter) dataXRreader).getDataSourceFactory().identityValue())) {
                    msgHandler.addFieldError(context, KEY_DB_NAME_FIELD_NAME, "源端和目标端不能相同，这非常危险！回头是岸啊");
                    return false;
                }
            }
            return true;
        }

        @Override
        protected final boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals form) {

            BasicDataXRdbmsWriter dataxWriter = (BasicDataXRdbmsWriter) form.newInstance();
            if (!validateSourceAndTargetEndIsSame(msgHandler, context, dataxWriter)) {
                return false;
            }


            return validatePostForm(msgHandler, context, dataxWriter);
        }

        protected boolean validatePostForm(
                IControlMsgHandler msgHandler, Context context, BasicDataXRdbmsWriter dataxWriter) {
            return true;
        }
    }

}
