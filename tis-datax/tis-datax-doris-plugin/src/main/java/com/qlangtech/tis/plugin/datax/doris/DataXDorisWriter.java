/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugin.datax.doris;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicDorisStarRocksWriter;
import com.qlangtech.tis.plugin.datax.common.InitWriterTable;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * reference: https://github.com/DorisDB/DataX/blob/master/doriswriter/doc/doriswriter.md
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-07 09:39
 * @see com.dorisdb.connector.datax.plugin.writer.doriswriter.DorisWriter
 **/
public class DataXDorisWriter extends BasicDorisStarRocksWriter {

//    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {})
//    public String loadProps;
//    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
//    public Integer maxBatchRows;
//
//    @Override
//    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
//        if (!tableMap.isPresent()) {
//            throw new IllegalStateException("tableMap must be present");
//        }
//        return new DorisWriterContext(this, tableMap.get());
//    }
//
//    public static String getDftTemplate() {
//        return IOUtils.loadResourceFromClasspath(DataXDorisWriter.class, "DataXDorisWriter-tpl.json");
//    }
//
//
//    /**
//     * 需要先初始化表starrocks目标库中的表
//     */
//    public void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
//        InitWriterTable.process(this.dataXName, targetTabName, jdbcUrls);
//    }
//
//    @Override
//    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
//        if (!this.autoCreateTable) {
//            return null;
//        }
//        // https://doris.apache.org/master/zh-CN/sql-reference/sql-statements/Data%20Definition/CREATE%20TABLE.html#create-table
//
//        final CreateTableSqlBuilder createTableSqlBuilder = new CreateTableSqlBuilder(tableMapper) {
//            @Override
//            protected void appendExtraColDef(List<ISelectedTab.ColMeta> pks) {
////                if (pk != null) {
////                    script.append("  PRIMARY KEY (`").append(pk.getName()).append("`)").append("\n");
////                }
//            }
//
//            @Override
//            protected void appendTabMeta(List<ISelectedTab.ColMeta> pks) {
//                script.append(" ENGINE=olap").append("\n");
//                if (pks.size() > 0) {
//                    script.append("UNIQUE KEY(").append(pks.stream()
//                            .map((pk) -> this.colEscapeChar() + pk.getName() + this.colEscapeChar())
//                            .collect(Collectors.joining(","))).append(")\n");
//                }
//                script.append("DISTRIBUTED BY HASH(");
//                if (pks.size() > 0) {
//                    script.append(pks.stream()
//                            .map((pk) -> this.colEscapeChar() + pk.getName() + this.colEscapeChar())
//                            .collect(Collectors.joining(",")));
//                } else {
//                    List<ISelectedTab.ColMeta> cols = this.getCols();
//                    Optional<ISelectedTab.ColMeta> firstCol = cols.stream().findFirst();
//                    if (firstCol.isPresent()) {
//                        script.append(firstCol.get().getName());
//                    } else {
//                        throw new IllegalStateException("can not find table:" + getCreateTableName() + " any cols");
//                    }
//                }
//                script.append(")\n");
//                script.append("BUCKETS 10\n");
//                script.append("PROPERTIES(\"replication_num\" = \"1\")");
//                //script.append("DISTRIBUTED BY HASH(customerregister_id)");
//            }
//
//            @Override
//            protected String convertType(ISelectedTab.ColMeta col) {
//                ColumnMetaData.DataType type = col.getType();
//                return type.accept(new ColumnMetaData.TypeVisitor<String>() {
//                    @Override
//                    public String longType(ColumnMetaData.DataType type) {
//                        return "BIGINT";
//                    }
//
//                    @Override
//                    public String doubleType(ColumnMetaData.DataType type) {
//                        return "DOUBLE";
//                    }
//
//                    @Override
//                    public String dateType(ColumnMetaData.DataType type) {
//                        return "DATE";
//                    }
//
//                    @Override
//                    public String timestampType(ColumnMetaData.DataType type) {
//                        return "DATETIME";
//                    }
//
//                    @Override
//                    public String bitType(ColumnMetaData.DataType type) {
//                        return "TINYINT";
//                    }
//
//                    @Override
//                    public String blobType(ColumnMetaData.DataType type) {
//                        return "BITMAP";
//                    }
//
//                    @Override
//                    public String varcharType(ColumnMetaData.DataType type) {
//                        return "VARCHAR(" + Math.min(type.columnSize, 65500) + ")";
//                    }
//
//                    @Override
//                    public String intType(ColumnMetaData.DataType type) {
//                        return "INT";
//                    }
//
//                    @Override
//                    public String floatType(ColumnMetaData.DataType type) {
//                        return "FLOAT";
//                    }
//
//                    @Override
//                    public String decimalType(ColumnMetaData.DataType type) {
//                        return "DECIMAL";
//                    }
//                });
//            }
//        };
//        return createTableSqlBuilder.build();
//    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDescriptor {
        public DefaultDescriptor() {
            super();
        }

//        @Override
//        protected int getMaxBatchSize() {
//            return Integer.MAX_VALUE;
//        }
//
//        @Override
//        public boolean isSupportTabCreate() {
//            return true;
//        }
//
//        public boolean validateLoadProps(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            try {
//                JSON.parseObject(value);
//                return true;
//            } catch (Exception e) {
//                msgHandler.addFieldError(context, fieldName, e.getMessage());
//                return false;
//            }
//        }
//
//        @Override
//        protected EndType getEndType() {
//            return EndType.StarRocks;
//        }

        @Override
        public String getDisplayName() {
            return DorisSourceFactory.NAME_DORIS;
        }
    }
}
