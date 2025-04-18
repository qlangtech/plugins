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

package com.qlangtech.tis.plugin.datax.dameng.writer;

import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html#3.5.1%20%E8%A1%A8%E5%AE%9A%E4%B9%89%E8%AF%AD%E5%8F%A5
 * <p>
 * https://eco.dameng.com/document/dm/zh-cn/sql-dev/dmpl-sql-datatype.html#%E6%97%A5%E6%9C%9F%E6%97%B6%E9%97%B4%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:01
 **/
public class DaMengAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        // ddl中timestamp字段个数不能大于1个要控制，第二个的时候要用datetime
        //final AtomicInteger timestampCount = new AtomicInteger();
        BasicDataXRdbmsWriter dataXWriter = (BasicDataXRdbmsWriter) rdbmsWriter;

        final CreateTableSqlBuilder createTableSqlBuilder
                = new CreateTableSqlBuilder<>(tableMapper, dataXWriter.getDataSourceFactory(), transformers) {
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
                super.appendTabMeta(pks);
                DaMengAutoCreateTable.this.getAddComment().addOracleLikeColComment(this, sourceColMetaGetter, tableMapper, script);
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c, this.pks) {
                    @Override
                    public String getMapperType() {
                        return convertType(this);
                    }
                };
            }

            /**
             * https://docs.oracle.com/database/121/SQLRF/sql_elements001.htm#SQLRF30020
             * https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm
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
        return createTableSqlBuilder;
    }

    @TISExtension
    public static class Desc extends ParamsAutoCreateTable.DftDesc {
        public Desc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.DaMeng;
        }
    }
}
