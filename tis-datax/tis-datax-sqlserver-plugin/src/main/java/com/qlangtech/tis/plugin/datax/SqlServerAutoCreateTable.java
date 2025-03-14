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

import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory.ISchemaSupported;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.ds.sqlserver.SqlServerCreateColumnMeta.KEY_MS_DESCRIPTION;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:27
 **/
public class SqlServerAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        BasicDataXRdbmsWriter dataXWriter = (BasicDataXRdbmsWriter) rdbmsWriter;
        final CreateTableSqlBuilder createTableSqlBuilder
                = new CreateTableSqlBuilder<>(tableMapper, dataXWriter.getDataSourceFactory(), transformers) {

            private boolean isMulitPks() {
                return this.pks.size() > 1;
            }

            private String convertType(DataType type, boolean isPk) {
                //https://www.cnblogs.com/liberty777/p/10748570.html
                StringBuffer createSql = new StringBuffer(getSqlServerType(type));

                if (!this.isMulitPks() && isPk) {
                    createSql.append(" primary key ");
                }
                return createSql.toString();
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c, this.pks) {
                    @Override
                    public String getMapperType() {
                        return convertType(this.getType(), this.isPk());
                    }
                };
            }

            private String getSqlServerType(DataType type) {

                switch (type.getJdbcType()) {
                    case INTEGER:
                    case TINYINT:
                    case SMALLINT:
                        return "int";
                    case BIGINT:
                        return "bigint";
                    case FLOAT:
                    case DOUBLE:
                    case DECIMAL:
                        return "decimal(" + type.getColumnSize() + ", " + type.getDecimalDigits() + ")";
                    case DATE:
                        return "date";
                    case TIME:
                    case TIMESTAMP:
                        return "datetime";
                    case BIT:
                    case BOOLEAN:
                        return "bit";
                    case BLOB:
                    case BINARY:
                    case LONGVARBINARY:
                    case VARBINARY:
                        //https://learn.microsoft.com/en-us/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16
                        // Variable-length binary data. n can be a value from 1 through 8,000.
                        // type.columnSize 可能为0 所以要用Math.max() 调整一下
                        return "varbinary(" + Math.min(Math.max(type.getColumnSize(), 300), 8000) + ")";
                    case LONGVARCHAR:
                        return "text";
                    default:
                        return "varchar(" + type.getColumnSize() + ")";
                }
            }


            @Override
            protected void appendExtraColDef(List<String> pk) {
                if (this.isMulitPks()) {
                    /**
                     * 建表语句中不能有超过一个列的修饰符为 “primary key”
                     * <pre>
                     * CREATE TABLE "base"
                     * (
                     *     "base_id"       int primary key ,
                     *     "start_time"    datetime,
                     *     "update_date"   datetime primary key ,
                     *     "update_time"   datetime,
                     *     "price"         decimal(5, 2),
                     *     "json_content"  varchar(2000),
                     *     "col_blob"      varbinary(8000),
                     *     "col_text"      text
                     * )
                     * </pre>
                     * 应该改为：
                     * <pre>
                     * CREATE TABLE "base"
                     * (
                     *     "base_id"       int  ,
                     *     "start_time"    datetime,
                     *     "update_date"   datetime ,
                     *     "update_time"   datetime,
                     *     "price"         decimal(5, 2),
                     *     "json_content"  varchar(2000),
                     *     "col_blob"      varbinary(8000),
                     *     "col_text"      text
                     *     ,PRIMARY KEY ( "base_id"  , "update_date")
                     * )
                     * </pre>
                     */
                    script.appendLine(",PRIMARY KEY ( " + pk.stream().map((key) -> this.dsMeta.getEscapedEntity(key)).collect(Collectors.joining(",")) + " )");
                }
            }

            @Override
            protected void appendTabMeta(List<String> pk) {
                super.appendTabMeta(pk);
                if (enabledColumnComment()) {
                    final ISchemaSupported schemaSupported = (ISchemaSupported) dsMeta;
                    for (ColWrapper col : this.getCols()) {

                        ColumnMetaData colMeta = sourceColMetaGetter.getColMeta(tableMapper, col.getName());
                        if (colMeta != null && StringUtils.isNotEmpty(colMeta.getComment())) {
                            script.blockBody(true, new String[]{StringUtils.EMPTY, StringUtils.EMPTY}
                                    , "EXEC sp_addextendedproperty", (buffer) -> {
                                        buffer.appendLine("@name = N'" + KEY_MS_DESCRIPTION + "',");
                                        buffer.appendLine("@value = N'" + colMeta.getComment() + "',");
                                        buffer.appendLine("@level0type = N'Schema', @level0name = '" + schemaSupported.getDBSchema() + "',");
                                        buffer.appendLine("@level1type = N'Table',  @level1name = '" + tableMapper.getTo() + "',");
                                        buffer.appendLine("@level2type = N'Column', @level2name = '" + col.getName() + "';");
                                    });
                        }
                    }
                }
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
            return EndType.SqlServer;
        }
    }
}
