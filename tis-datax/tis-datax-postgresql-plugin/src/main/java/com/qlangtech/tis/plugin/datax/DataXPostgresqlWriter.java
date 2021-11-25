/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.postgresqlwriter.PostgresqlWriter
 **/
public class DataXPostgresqlWriter extends BasicDataXRdbmsWriter<PGDataSourceFactory> {


//    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String jdbcUrl;
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String username;
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String password;
//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String table;
//    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String column;
//    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
//    public String preSql;
//    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
//    public String postSql;
//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
//    public String batchSize;
//
//    @FormField(ordinal = 8, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXPostgresqlWriter.class, "DataXPostgresqlWriter-tpl.json");
    }

    public static void main(String[] args) {
        System.out.println(StringUtils.EMPTY.toCharArray()[0]);
    }

    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }

        // 多个主键
        boolean multiPk = tableMapper.getSourceCols().stream().filter((col) -> col.isPk()).count() > 1;

        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper) {

            @Override
            protected void appendExtraColDef(List<ISelectedTab.ColMeta> pks) {
//                if (!pks.isEmpty()) {
//                    script.append("  PRIMARY KEY (").append(pks.stream().map((pk) -> "`" + pk.getName() + "`")
//                            .collect(Collectors.joining(","))).append(")").append("\n");
//                }
                if (multiPk) {
                    this.script.append(", CONSTRAINT ").append("uk_" + getCreateTableName() + "_unique_" + pks.stream().map((c) -> c.getName()).collect(Collectors.joining("_")))
                            .append(" UNIQUE(")
                            .append(pks.stream().map((c) -> c.getName()).collect(Collectors.joining(","))).append(")");
                }
            }

            @Override
            protected char colEscapeChar() {
                return '\"';
            }


            @Override
            protected void appendTabMeta(List<ISelectedTab.ColMeta> pks) {

            }

            /**
             * https://www.runoob.com/mysql/mysql-data-types.html
             * @param col
             * @return
             */
            @Override
            protected String convertType(ISelectedTab.ColMeta col) {
                ColumnMetaData.DataType type = col.getType();
                String colType = type.accept(new ColumnMetaData.TypeVisitor<String>() {
                    @Override
                    public String longType(ColumnMetaData.DataType type) {
                        return "BIGINT";
                    }

                    @Override
                    public String doubleType(ColumnMetaData.DataType type) {
                        return "FLOAT8";
                    }

                    @Override
                    public String dateType(ColumnMetaData.DataType type) {
                        return "DATE";
                    }

                    @Override
                    public String timestampType(ColumnMetaData.DataType type) {
                        return "TIMESTAMP";
                    }

                    @Override
                    public String bitType(ColumnMetaData.DataType type) {
                        return "BIT";
                    }

                    @Override
                    public String blobType(ColumnMetaData.DataType type) {
                        return "BYTEA";
                    }

                    @Override
                    public String varcharType(ColumnMetaData.DataType type) {
                        return "VARCHAR(" + type.columnSize + ")";
                    }

                    @Override
                    public String intType(ColumnMetaData.DataType type) {
                        return "INTEGER";
                    }

                    @Override
                    public String floatType(ColumnMetaData.DataType type) {
                        return "FLOAT4";
                    }

                    @Override
                    public String decimalType(ColumnMetaData.DataType type) {
                        return "DECIMAL";
                    }

                    @Override
                    public String timeType(ColumnMetaData.DataType type) {
                        return "TIME";
                    }

                    @Override
                    public String tinyIntType(ColumnMetaData.DataType dataType) {
                        return smallIntType(dataType);
                    }

                    @Override
                    public String smallIntType(ColumnMetaData.DataType dataType) {
                        return "SMALLINT";
                    }
                });

                return colType + (!multiPk && col.isPk() ? " PRIMARY KEY" : StringUtils.EMPTY);
            }

        };
        return createTableSqlBuilder.build();
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        PostgreWriterContext writerContext = new PostgreWriterContext(this, tableMap.get());

        return writerContext;
    }


    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DataXPostgresqlReader.PG_NAME;
        }
    }
}
