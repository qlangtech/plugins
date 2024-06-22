package com.qlangtech.tis.plugin.datax.dameng.writer;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.reader.DataXDaMengReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/14
 */
public class DataXDaMengWriter extends BasicDataXRdbmsWriter<DaMengDataSourceFactory> {
    private static final String DATAX_NAME = DataXDaMengReader.DATAX_NAME;
    private static final Logger logger = LoggerFactory.getLogger(DataXDaMengWriter.class);

//    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String writeMode;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDaMengWriter.class, "dameng-writer-tpl.json");
    }


    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        DaMengDataSourceFactory dsFactory = this.getDataSourceFactory();
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
        DaMengWriterContext context = new DaMengWriterContext(this.dataXName);
        if (dataDumpers.dumpers.hasNext()) {
            IDataSourceDumper next = dataDumpers.dumpers.next();
            context.setJdbcUrl(next.getDbHost());
            context.setPassword(dsFactory.password);
            context.setUsername(dsFactory.userName);
            context.setTabName(table.getTableName());
            context.cols = IDataxProcessor.TabCols.create(new IDBReservedKeys() {
            }, tm);
            context.setDbName(this.dbName);
            //    context.writeMode = this.writeMode;
            context.setPreSql(this.preSql);
            context.setPostSql(this.postSql);
            context.setSession(session);
            context.setBatchSize(batchSize);
            return context;
        }

        throw new RuntimeException("dbName:" + dbName + " relevant DS is empty");
    }

    /**
     * https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html#3.5.1%20%E8%A1%A8%E5%AE%9A%E4%B9%89%E8%AF%AD%E5%8F%A5
     * <p>
     * https://eco.dameng.com/document/dm/zh-cn/sql-dev/dmpl-sql-datatype.html#%E6%97%A5%E6%9C%9F%E6%97%B6%E9%97%B4%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B
     *
     * @param tableMapper
     * @return
     */
    @Override
    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        //        if (!this.autoCreateTable) {
        //            return null;
        //        }
//        StringBuffer script = new StringBuffer();
//        DataxReader threadBingDataXReader = DataxReader.getThreadBingDataXReader();
//        Objects.requireNonNull(threadBingDataXReader, "getThreadBingDataXReader can not be null");
//        try {
//            if (threadBingDataXReader instanceof DataXDaMengReader
//                    // 没有使用别名
//                    && tableMapper.hasNotUseAlias()) {
//                DataXDaMengReader mySQLReader = (DataXDaMengReader) threadBingDataXReader;
//                DaMengDataSourceFactory dsFactory = mySQLReader.getDataSourceFactory();
//                dsFactory.visitFirstConnection((c) -> {
//                    Connection conn = c.getConnection();
//                    DataXJobInfo jobInfo = dsFactory.getTablesInDB().createDataXJobInfo(//
//                            DataXJobSubmit.TableDataXEntity.createTableEntity(null, c.getUrl(), tableMapper.getFrom()));
//                    Optional<String[]> physicsTabNames = jobInfo.getTargetTableNames();
//                    if (physicsTabNames.isPresent()) {
//                        String sourceTabName = physicsTabNames.get()[0];
//                        try (Statement statement = conn.createStatement()) {
//                            // FIXME: 如果源端是表是分表，则在Sink端需要用户自行将DDL的表名改一下
//                            //
//                            try (ResultSet resultSet =
//                                         statement.executeQuery("SELECT TABLEDEF('TIS','" + sourceTabName + "')")) {
//                                if (!resultSet.next()) {
//                                    throw new IllegalStateException("table:" + tableMapper.getFrom() + " can not " +
//                                            "exec" + " show create table script");
//                                }
//                                String ddl = resultSet.getString(2);
//                                script.append(ddl);
//                            }
//                        }
//                    } else {
//                        throw new IllegalStateException("table:" + tableMapper.getFrom() + " can not find " +
//                                "physicsTabs" + " in datasource:" + dsFactory.identityValue());
//                    }
//
//                });
//                return new CreateTableSqlBuilder.CreateDDL(script, null) {
//                    @Override
//                    public String getSelectAllScript() {
//                        //return super.getSelectAllScript();
//                        throw new UnsupportedOperationException();
//                    }
//                };
//            }
//        } catch (RuntimeException e) {
//            if (ExceptionUtils.indexOfThrowable(e, TableNotFoundException.class) < 0) {
//                throw e;
//            } else {
//                // 当Reader 的MySQL Source端中采用为分表策略，则会取不到表，直接采用一下基于metadata数据来生成DDL
//                logger.warn("table:" + tableMapper.getFrom() + " is not exist in Reader Source");
//            }
//        }

        // ddl中timestamp字段个数不能大于1个要控制，第二个的时候要用datetime
        //final AtomicInteger timestampCount = new AtomicInteger();

        CreateTableSqlBuilder.CreateDDL createDDL = null;

        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper, this.getDataSourceFactory(),transformers) {
            @Override
            protected void appendExtraColDef(List<String> pks) {
                if (CollectionUtils.isEmpty(pks)) {
                    return;
                }
                script.append(" , CONSTRAINT ").append(tableMapper.getTo()).append("_pk PRIMARY KEY (")
                        .append(pks.stream().map((pk) -> wrapWithEscape(pk))
                                .collect(Collectors.joining(","))).append(")").append("\n");
            }

            @Override
            protected void appendTabMeta(List<String> pks) {
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
             * https://docs.oracle.com/database/121/SQLRF/sql_elements001.htm#SQLRF30020
             * https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm
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
                        return "NUMBER(1,0)";
                    case REAL: {
                        if (type.getColumnSize() > 0 && type.getDecimalDigits() > 0) {
                            // 在PG->Oracle情况下，PG中是Real类型 通过jdbc反射得到columnSize和getDecimalDigits()都为8，这样number(8,8)就没有小数位了，出问题了
                            // 在此进行除2处理
                            int scale = type.getDecimalDigits();
                            if (scale >= type.getColumnSize()) {
                                scale = scale / 2;
                            }
                            return "NUMBER(" + type.getColumnSize() + "," + scale + ")";
                        }
                        return "BINARY_FLOAT";
                    }
                    case TINYINT:
                    case SMALLINT:
                        return "SMALLINT";
                    case INTEGER:
                        return "INTEGER";
                    case BIGINT:
                        return "BIGINT";
                    case FLOAT:
                        return "BINARY_FLOAT";
                    case DOUBLE:
                        return "BINARY_DOUBLE";
                    case DECIMAL:
                    case NUMERIC: {
                        if (type.getColumnSize() > 0) {
                            return "DECIMAL(" + Math.min(type.getColumnSize(), 38) + "," + type.getDecimalDigits() + ")";
                        } else {
                            return "DECIMAL";
                        }
                    }
                    case DATE:
                        return "DATE";
                    case TIME:
                        return "TIMESTAMP(0)";
                    // return "TIME";
                    case TIMESTAMP:
                        return "TIMESTAMP";
                    case BLOB:
                    case BINARY:
                    case LONGVARBINARY:
                    case VARBINARY:
                        return "BLOB";
                    case VARCHAR: {
                        if (type.getColumnSize() > Short.MAX_VALUE) {
                            return "CLOB";
                        }
                        return "VARCHAR2(" + type.getColumnSize() + " CHAR)";
                    }
                    default:
                        // return "TINYTEXT";
                        return "CLOB";
                }
            }


        };
        createDDL = createTableSqlBuilder.build();
        return createDDL;
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
            return EndType.DaMeng;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        protected boolean validatePostForm(IControlMsgHandler msgHandler, Context context, BasicDataXRdbmsWriter form) {

            DataXDaMengWriter dataxWriter = (DataXDaMengWriter) form;
            DaMengDataSourceFactory dsFactory = dataxWriter.getDataSourceFactory();
            if (dsFactory.splitTableStrategy.isSplittable()) {
                msgHandler.addFieldError(context, KEY_DB_NAME_FIELD_NAME, "Writer端不能使用带有分表策略的数据源");
                return false;
            }

            return super.validatePostForm(msgHandler, context, dataxWriter);
        }
    }
}
