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
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.mysqlwriter.TISMysqlWriter
 **/
@Public
public class DataxMySQLWriter extends BasicDataXRdbmsWriter implements IWriteModeSupport {
    private static final String DATAX_NAME = "MySQL";
    private static final Logger logger = LoggerFactory.getLogger(DataxMySQLWriter.class);

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataxMySQLReader.class, "mysql-writer-tpl.json");
    }

    @Override
    public WriteMode getWriteMode() {
        return WriteMode.parse(this.writeMode);
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        MySQLDataSourceFactory dsFactory = (MySQLDataSourceFactory) this.getDataSourceFactory();
        IDataxProcessor.TableMap tm = tableMap.get();
        if (CollectionUtils.isEmpty(tm.getSourceCols())) {
            throw new IllegalStateException("tablemap " + tm + " source cols can not be null");
        }
        TISTable table = new TISTable();
        table.setTableName(tm.getTo());
        DataDumpers dataDumpers = dsFactory.getDataDumpers(table);
        if (dataDumpers.splitCount > 1) {
            // 写入库还支持多组路由的方式分发，只能向一个目标库中写入
            throw new IllegalStateException("dbSplit can not max than 1");
        }
        MySQLWriterContext context = new MySQLWriterContext(this.dataXName);
        if (dataDumpers.dumpers.hasNext()) {
            IDataSourceDumper next = dataDumpers.dumpers.next();
            context.jdbcUrl = next.getDbHost();
            context.password = dsFactory.password;
            context.username = dsFactory.userName;
            context.tabName = table.getTableName();
            context.cols = IDataxProcessor.TabCols.create( dsFactory, tm, transformerRules);
            context.dbName = this.dbName;
            context.writeMode = this.writeMode;
            context.preSql = this.preSql;
            context.postSql = this.postSql;
            context.session = session;
            context.batchSize = batchSize;
            return context;
        }

        throw new RuntimeException("dbName:" + dbName + " relevant DS is empty");
    }

    @Override
    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {


        StringBuffer script = new StringBuffer();
        DataxReader threadBingDataXReader = DataxReader.getThreadBingDataXReader();
        Objects.requireNonNull(threadBingDataXReader, "getThreadBingDataXReader can not be null");
        try {
            if (threadBingDataXReader instanceof DataxMySQLReader //
                    // 没有使用别名
                    && tableMapper.hasNotUseAlias() //
                    && !transformers.isPresent()) {
                DataxMySQLReader mySQLReader = (DataxMySQLReader) threadBingDataXReader;
                MySQLDataSourceFactory dsFactory = mySQLReader.getDataSourceFactory();
                dsFactory.visitFirstConnection((c) -> {
                    Connection conn = c.getConnection();
                    DataXJobInfo jobInfo = dsFactory.getTablesInDB().createDataXJobInfo(//
                            DataXJobSubmit.TableDataXEntity.createTableEntity(null, c.getUrl(), tableMapper.getFrom()));
                    Optional<String[]> physicsTabNames = jobInfo.getTargetTableNames();
                    if (physicsTabNames.isPresent()) {
                        try (Statement statement = conn.createStatement()) {
                            // FIXME: 如果源端是表是分表，则在Sink端需要用户自行将DDL的表名改一下
                            try (ResultSet resultSet =
                                         statement.executeQuery("show create table " + dsFactory.getEscapedEntity(physicsTabNames.get()[0]))) {
                                if (!resultSet.next()) {
                                    throw new IllegalStateException("table:" + tableMapper.getFrom() + " can not " +
                                            "exec" + " show create table script");
                                }
                                String ddl = resultSet.getString(2);
                                script.append(ddl);
                            }
                        }
                    } else {
                        throw new IllegalStateException("table:" + tableMapper.getFrom() + " can not find " +
                                "physicsTabs" + " in datasource:" + dsFactory.identityValue());
                    }

                });
                return new CreateTableSqlBuilder.CreateDDL(script, null) {
                    @Override
                    public String getSelectAllScript() {
                        //return super.getSelectAllScript();
                        throw new UnsupportedOperationException();
                    }
                };
            }
        } catch (RuntimeException e) {
            if (ExceptionUtils.indexOfThrowable(e, TableNotFoundException.class) < 0) {
                throw e;
            } else {
                // 当Reader 的MySQL Source端中采用为分表策略，则会取不到表，直接采用一下基于metadata数据来生成DDL
                logger.warn("table:" + tableMapper.getFrom() + " is not exist in Reader Source");
            }
        }

        // ddl中timestamp字段个数不能大于1个要控制，第二个的时候要用datetime
        final AtomicInteger timestampCount = new AtomicInteger();

        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper,
                this.getDataSourceFactory(), transformers) {
            @Override
            protected void appendExtraColDef(List<String> pks) {
                if (!pks.isEmpty()) {
                    script.append(" , PRIMARY KEY (").append(pks.stream().map((pk) -> "`" + pk + "`").collect(Collectors.joining(","))).append(")").append("\n");
                }
            }

            @Override
            protected void appendTabMeta(List<String> pks) {
                script.append(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci").append("\n");
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c) {
                    @Override
                    public String getMapperType() {
                        return convertType(this.meta);
                    }
                };
            }

            /**
             * https://www.runoob.com/mysql/mysql-data-types.html
             * @param col
             * @return
             */
            private String convertType(IColMetaGetter col) {
                DataType type = col.getType();
                switch (type.getJdbcType()) {
                    case CHAR: {
                        String keyChar = "CHAR";
                        if (type.getColumnSize() < 1) {
                            return keyChar;
                        }
                        return keyChar + "(" + type.getColumnSize() + ")";
                    }
                    case BIT:
                    case BOOLEAN:
                        return "BOOLEAN";
                    case REAL:
                        return "REAL";
                    case TINYINT: {
                        return "TINYINT(" + type.getColumnSize() + ") " + type.getUnsignedToken();
                    }
                    case SMALLINT: {
                        return "SMALLINT(" + type.getColumnSize() + ") " + type.getUnsignedToken();
                    }
                    case INTEGER:
                        return "int(11)";
                    case BIGINT: {
                        return "BIGINT " + type.getUnsignedToken();
                    }
                    case FLOAT:
                        return "FLOAT";
                    case DOUBLE:
                        return "DOUBLE";
                    case DECIMAL:
                    case NUMERIC: {
                        if (type.getColumnSize() > 0) {
                            return "DECIMAL(" + type.getColumnSize() + "," + type.getDecimalDigits() + ")";
                        } else {
                            return "DECIMAL(" + DataTypeMeta.DEFAULT_DECIMAL_PRECISION + ",0)";
                        }
                    }
                    case DATE:
                        return "DATE";
                    case TIME:
                        return "TIME";
                    case TIMESTAMP: {
                        if (timestampCount.getAndIncrement() < 1) {
                            return "TIMESTAMP";
                        } else {
                            return "DATETIME";
                        }
                    }
                    case BLOB:
                    case BINARY:
                    case LONGVARBINARY:
                    case VARBINARY:
                        return "BLOB";
                    case VARCHAR: {
                        if (type.getColumnSize() > Short.MAX_VALUE) {
                            return "TEXT";
                        }
                        return "VARCHAR(" + type.getColumnSize() + ")";
                    }
                    default:
                        return "TINYTEXT";
                }
            }
        };
        return createTableSqlBuilder.build();
    }


    public static class MySQLWriterContext extends RdbmsDataxContext implements IDataxContext {

        public MySQLWriterContext(String dataXName) {
            super(dataXName);
        }

        private String dbName;
        private String writeMode;
        private String preSql;
        private String postSql;
        private String session;
        private Integer batchSize;

        public String getDbName() {
            return dbName;
        }

        public String getWriteMode() {
            return writeMode;
        }

        public String getPreSql() {
            return preSql;
        }

        public String getPostSql() {
            return postSql;
        }

        public String getSession() {
            return session;
        }

        public boolean isContainPreSql() {
            return StringUtils.isNotBlank(preSql);
        }

        public boolean isContainPostSql() {
            return StringUtils.isNotBlank(postSql);
        }

        public boolean isContainSession() {
            return StringUtils.isNotBlank(session);
        }

        public Integer getBatchSize() {
            return batchSize;
        }
    }


    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }


        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return EndType.MySQL;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        protected boolean validatePostForm(IControlMsgHandler msgHandler, Context context, BasicDataXRdbmsWriter form) {

            DataxMySQLWriter dataxWriter = (DataxMySQLWriter) form;
            MySQLDataSourceFactory dsFactory = (MySQLDataSourceFactory) dataxWriter.getDataSourceFactory();
            // if (!(dsFactory.splitTableStrategy instanceof NoneSplitTableStrategy)) {
            if (dsFactory.splitTableStrategy.isSplittable()) {
                msgHandler.addFieldError(context, KEY_DB_NAME_FIELD_NAME, "Writer端不能使用带有分表策略的数据源");
                return false;
            }

            return super.validatePostForm(msgHandler, context, dataxWriter);
        }

        //        @Override
        //        public SuFormProperties overwriteSubPluginFormPropertyTypes(SuFormProperties subformProps) throws
        //        Exception {
        //
        //            final String targetClass = MySQLSelectedTab.class.getName();
        //
        //            Descriptor newSubDescriptor = Objects.requireNonNull(TIS.get().getDescriptor(targetClass)
        //                    , "subForm clazz:" + targetClass + " can not find relevant Descriptor");
        //
        //            SuFormProperties rewriteSubFormProperties = SuFormProperties.copy(
        //                    filterFieldProp(buildPropertyTypes(Optional.of(newSubDescriptor), MySQLSelectedTab.class))
        //                    , MySQLSelectedTab.class
        //                    , newSubDescriptor
        //                    , subformProps);
        //            return rewriteSubFormProperties;
        //
        //        }
    }
}
