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
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.oraclewriter.TISOracleWriter
 **/
@Public
public class DataXOracleWriter extends BasicDataXRdbmsWriter<OracleDataSourceFactory> {


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXOracleWriter.class, "DataXOracleWriter-tpl.json");
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        OracleWriterContext writerContext = new OracleWriterContext(this, tableMap.get(), transformerRules);
        return writerContext;
    }

//    @Override
//    public void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
//        InitWriterTable.process(this.dataXName, targetTabName, jdbcUrls);
//    }

//    /**
//     * https://docs.oracle.com/cd/B28359_01/server.111/b28318/sqlplsql.htm#CNCPT1732
//     *
//     * @param tableMapper
//     * @return
//     */
//    @Override
//    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(SourceColMetaGetter sourceColMetaGetter
//            , IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
//
//        CreateTableSqlBuilder.CreateDDL createDDL = null;
//
//        final CreateTableSqlBuilder createTableSqlBuilder
//                = new CreateTableSqlBuilder<ColWrapper>(tableMapper, this.getDataSourceFactory(), transformers) {
//            @Override
//            protected void appendExtraColDef(List<String> pks) {
//                if (pks.isEmpty()) {
//                    return;
//                }
//                script.append(" , CONSTRAINT ").append(tableMapper.getTo()).append("_pk PRIMARY KEY (")
//                        .append(pks.stream().map((pk) -> wrapWithEscape(pk))
//                                .collect(Collectors.joining(","))).append(")").append("\n");
//            }
//
//            @Override
//            protected void appendTabMeta(List<String> pks) {
//                super.appendTabMeta(pks);
//                autoCreateTable.addOracleLikeColComment(this, sourceColMetaGetter, tableMapper, script);
//            }
//
//            @Override
//            protected ColWrapper createColWrapper(IColMetaGetter c) {
//                return new ColWrapper(c, this.pks) {
//                    @Override
//                    public String getMapperType() {
//                        return convertType(this);
//                    }
//                };
//            }
//
//            /**
//             * https://docs.oracle.com/database/121/SQLRF/sql_elements001.htm#SQLRF30020
//             * https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm
//             * @param col
//             * @return
//             */
//            private String convertType(ColWrapper col) {
//                DataType type = col.getType();
//                switch (type.getJdbcType()) {
//                    case CHAR: {
//                        String keyChar = "CHAR";
//                        if (type.getColumnSize() < 1) {
//                            return keyChar;
//                        }
//                        return keyChar + "(" + type.getColumnSize() + ")";
//                    }
//                    case BIT:
//                    case BOOLEAN:
//                        return "NUMBER(1,0)";
//                    case REAL: {
//                        if (type.getColumnSize() > 0 && type.getDecimalDigits() > 0) {
//                            // 在PG->Oracle情况下，PG中是Real类型 通过jdbc反射得到columnSize和getDecimalDigits()都为8，这样number(8,8)就没有小数位了，出问题了
//                            // 在此进行除2处理
//                            int scale = type.getDecimalDigits();
//                            if (scale >= type.getColumnSize()) {
//                                scale = scale / 2;
//                            }
//                            return "NUMBER(" + type.getColumnSize() + "," + scale + ")";
//                        }
//                        return "BINARY_FLOAT";
//                    }
//                    case TINYINT:
//                    case SMALLINT:
//                        return "SMALLINT";
//                    case INTEGER:
//                    case BIGINT:
//                        return "INTEGER";
//                    case FLOAT:
//                        return "BINARY_FLOAT";
//                    case DOUBLE:
//                        return "BINARY_DOUBLE";
//                    case DECIMAL:
//                    case NUMERIC: {
//                        if (type.getColumnSize() > 0) {
//                            return "DECIMAL(" + Math.min(type.getColumnSize(), 38) + "," + type.getDecimalDigits() + ")";
//                        } else {
//                            return "DECIMAL";
//                        }
//                    }
//                    case DATE:
//                        return "DATE";
//                    case TIME:
//                        return "TIMESTAMP(0)";
//                    // return "TIME";
//                    case TIMESTAMP:
//                        return "TIMESTAMP";
//                    case BLOB:
//                    case BINARY:
//                    case LONGVARBINARY:
//                    case VARBINARY:
//                        return "BLOB";
//                    case VARCHAR: {
//                        if (type.getColumnSize() > Short.MAX_VALUE) {
//                            return "CLOB";
//                        }
//                        return "VARCHAR2(" + type.getColumnSize() + " CHAR)";
//                    }
//                    default:
//                        // return "TINYTEXT";
//                        return "CLOB";
//                }
//            }
//
//
//        };
//        createDDL = createTableSqlBuilder.build();
//        return createDDL;
//    }

    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        @Override
        public String getDisplayName() {
            return OracleDataSourceFactory.ORACLE;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public boolean isSupportIncr() {
//            try {
//                Method gDDL = this.clazz.getMethod("generateCreateDDL", IDataxProcessor.TableMap.class);
//                return gDDL.getDeclaringClass() != IDataxWriter.class;
//            } catch (NoSuchMethodException e) {
//                throw new RuntimeException(e);
//            }
            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Oracle;
        }

        public boolean validateSession(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXOracleReader.validateSession(msgHandler, context, fieldName, value);
        }
    }
}
