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

import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.AbstractCreateTableSqlBuilder.CreateDDL;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:30
 **/
public class MySQLAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {
    private static final Logger logger = LoggerFactory.getLogger(MySQLAutoCreateTable.class);

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {


        CreateTableSqlBuilder directBuilderByMySQLMeta
                = this.directCreateByMySQLMeta(rdbmsWriter, sourceColMetaGetter, tableMapper, transformers);

        if (directBuilderByMySQLMeta != null) {
            return directBuilderByMySQLMeta;
        }
        return new MySQLCreateTableSqlBuilder(sourceColMetaGetter, tableMapper, rdbmsWriter, transformers);
    }


    protected class MySQLCreateTableSqlBuilder extends CreateTableSqlBuilder<ColWrapper> {
        private final AtomicInteger timestampCount = new AtomicInteger();
        private final SourceColMetaGetter sourceColMetaGetter;
        private final TableMap _tableMapper;


        public MySQLCreateTableSqlBuilder(SourceColMetaGetter sourceColMetaGetter
                , TableMap tableMapper, DataxWriter rdbmsWriter, Optional<RecordTransformerRules> transformers) {
            super(tableMapper, ((BasicDataXRdbmsWriter) rdbmsWriter).getDataSourceFactory(), transformers);
            this._tableMapper = tableMapper;
            this.sourceColMetaGetter = sourceColMetaGetter;
        }

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
        protected final ColWrapper createColWrapper(IColMetaGetter c) {
            return new ColWrapper(c, this.pks) {
                @Override
                protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                    addComment.addStandardColComment(sourceColMetaGetter, _tableMapper, this, ddlScript);
                }

                @Override
                public String getMapperType() {
                    return convertType(this);
                }
            };
        }

        /**
         * https://www.runoob.com/mysql/mysql-data-types.html
         *
         * @param col
         * @return
         */
        private String convertType(ColWrapper col) {
            DataType type = col.getType();
            switch (type.getJdbcType()) {
                case CHAR: {
                    String keyChar = "CHAR";
                    if (type.getColumnSize() < 1) {
                        return keyChar;
                    }
                    return keyChar + "(" + type.getColumnSize() + ")";
                }
                case BIT: {
                    if (type.getColumnSize() > 1) {
                        return "BIT(" + type.getColumnSize() + ")";
                    }
                    //TINYINT 的存储大小固定为 1字节，无论括号中的数字是多少。其取值范围始终为：
                    //*有符号（Signed）：-128 到 127
                    //*无符号（Unsigned）：0 到 255
                    // 括号中的数字代表在客户端工具（如命令行、某些GUI工具）中显示时最小占用字符数的可选参数。
                    // 例如，TINYINT(4) 表示数值在显示时至少占用4个字符宽度。
                    /**
                     * 此处需要使用2，如果源端（mysql）中为TINYINT(1) 映射到 kingbase中也为TINYINT(1) 则会被视为Boolean类型，则会报以下错误：
                     * <pre>
                     *     was aborted: ERROR: column "is_limittime" is of type boolean but expression is of type bit varying
                     *   Hint: You will need to rewrite or cast the expression.
                     * </pre>
                     */
                    return "TINYINT(2)";
                }
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
    }

    /**
     * 当source 为MySQL 则可以直接从source的 meta中直接将create table的DDL 取得
     *
     * @param tableMapper
     * @param transformers
     * @return
     */
    protected CreateTableSqlBuilder directCreateByMySQLMeta(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter, TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        final StringBuffer ddlScript = new StringBuffer();
        DataxReader threadBingDataXReader = DataxReader.getThreadBingDataXReader();
        Objects.requireNonNull(threadBingDataXReader, "getThreadBingDataXReader can not be null");
        AtomicBoolean usingMySqlCreateDDLDirectly = new AtomicBoolean(false);
        try {
            // 默认必须执行
            if (false && (threadBingDataXReader instanceof DataxMySQLReader //
                    // 没有使用别名
                    // && tableMapper.hasNotUseAlias() //
                    && !transformers.isPresent())) {
                DataxMySQLReader mySQLReader = (DataxMySQLReader) threadBingDataXReader;
                MySQLDataSourceFactory dsFactory = mySQLReader.getDataSourceFactory();
                List<ColumnMetaData> tableColsMeta = mySQLReader.getTableMetadata(EntityName.parse(tableMapper.getFrom()));
                ISelectedTab selectedTab = mySQLReader.getSelectedTab(tableMapper.getFrom());

                if (StringUtils.equalsIgnoreCase(
                        selectedTab.getCols().stream().map((col) -> col.getName()).collect(Collectors.joining())
                        , tableColsMeta.stream().map((col) -> col.getName()).collect(Collectors.joining()))) {
                    // 确保没有导出列
                    dsFactory.visitFirstConnection((c) -> {
                        Connection conn = c.getConnection();
                        DataXJobInfo jobInfo = dsFactory.getTablesInDB().createDataXJobInfo(//
                                DataXJobSubmit.TableDataXEntity.createTableEntity(null, c.getUrl(), tableMapper.getFrom()), false);
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
                                    final String ddl = CreateDDL.replaceDDLTableName(resultSet.getString(2)
                                            , dsFactory.getEscapedEntity(tableMapper.getTo()));
                                    ddlScript.append(ddl);
                                }
                            }
                        } else {
                            throw new IllegalStateException("table:" + tableMapper.getFrom() + " can not find " +
                                    "physicsTabs" + " in datasource:" + dsFactory.identityValue());
                        }

                    });
                    usingMySqlCreateDDLDirectly.set(true);
                }
            }
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            if (ExceptionUtils.indexOfThrowable(e, TableNotFoundException.class) < 0) {
                throw e;
            } else {
                // 当Reader 的MySQL Source端中采用为分表策略，则会取不到表，直接采用一下基于metadata数据来生成DDL
                logger.warn("table:" + tableMapper.getFrom() + " is not exist in Reader Source");
            }
        }

        if (!usingMySqlCreateDDLDirectly.get()) {
            return null;
        }

        final CreateTableSqlBuilder createTableSqlBuilder = new MySQLCreateTableSqlBuilder(sourceColMetaGetter, tableMapper,
                rdbmsWriter, transformers) {
            @Override
            public CreateDDL build() {
                return new CreateTableSqlBuilder.CreateDDL(ddlScript, null) {
                    @Override
                    public String getSelectAllScript() {
                        //return super.getSelectAllScript();
                        throw new UnsupportedOperationException();
                    }
                };
            }

        };
        return createTableSqlBuilder;
    }

    @TISExtension
    public static class Desc extends ParamsAutoCreateTable.DftDesc {
        public Desc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.MySQL;
        }
    }
}
