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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.InitWriterTable;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-29 14:46
 **/
public abstract class BasicDorisStarRocksWriter<DS extends DorisSourceFactory> extends BasicDataXRdbmsWriter<DS> {

    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String loadProps;
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxBatchRows;

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        return new DorisWriterContext(this, tableMap.get());
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(BasicDorisStarRocksWriter.class, "BasicDorisStarRocksWriter-tpl.json");
    }

    public static String getDftLoadProps() {
        return "{\n" +
                "    \"" + Separator.COL_SEPARATOR + "\": \"" + Separator.COL_SEPARATOR_DEFAULT + "\",\n" +
                "    \"" + Separator.ROW_DELIMITER + "\": \"" + Separator.ROW_DELIMITER_DEFAULT + "\"\n" +
                "}";
    }

    public Separator getSeparator() {
        JSONObject props = getLoadProps();
        return new Separator() {
            @Override
            public String getColumnSeparator() {
                return StringUtils.defaultIfBlank(props.getString(Separator.COL_SEPARATOR), COL_SEPARATOR_DEFAULT);
            }

            @Override
            public String getRowDelimiter() {
                return StringUtils.defaultIfBlank(props.getString(Separator.ROW_DELIMITER), ROW_DELIMITER_DEFAULT);
            }
        };
    }

    public JSONObject getLoadProps() {
        return JSON.parseObject(loadProps);
    }


    public interface Separator {
        String COL_SEPARATOR = "column_separator";
        String ROW_DELIMITER = "row_delimiter";

        String COL_SEPARATOR_DEFAULT = "\\\\x01";
        String ROW_DELIMITER_DEFAULT = "\\\\x02";

        String getColumnSeparator();

        String getRowDelimiter();
    }


    /**
     * 需要先初始化表starrocks目标库中的表
     */
    public void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
        InitWriterTable.process(this.dataXName, targetTabName, jdbcUrls);
    }

    @Override
    public final CreateTableSqlBuilder.CreateDDL generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }
        // https://doris.apache.org/master/zh-CN/sql-reference/sql-statements/Data%20Definition/CREATE%20TABLE.html#create-table

        final BasicCreateTableSqlBuilder createTableSqlBuilder = createSQLDDLBuilder(tableMapper);

        return createTableSqlBuilder.build();
    }

    protected abstract BasicCreateTableSqlBuilder createSQLDDLBuilder(IDataxProcessor.TableMap tableMapper);


    protected static abstract class BasicCreateTableSqlBuilder extends CreateTableSqlBuilder {
        public BasicCreateTableSqlBuilder(IDataxProcessor.TableMap tableMapper) {
            super(tableMapper);
        }

        @Override
        protected void appendExtraColDef(List<ColWrapper> pks) {
//                if (pk != null) {
//                    script.append("  PRIMARY KEY (`").append(pk.getName()).append("`)").append("\n");
//                }
        }

        protected abstract String getUniqueKeyToken();


        @Override
        protected List<ColWrapper> preProcessCols(List<ColWrapper> pks, List<ISelectedTab.ColMeta> cols) {
            // 将主键排在最前面
            List<ColWrapper> result = Lists.newArrayList(pks);
            cols.stream().filter((c) -> !c.isPk()).forEach((c) -> {
                result.add(createColWrapper(c));
            });
            return result;
        }

        @Override
        protected void appendTabMeta(List<ColWrapper> pks) {
            script.append(" ENGINE=olap").append("\n");
            if (pks.size() > 0) {
                script.append(getUniqueKeyToken() + "(").append(pks.stream()
                        .map((pk) -> this.colEscapeChar() + pk.getName() + this.colEscapeChar())
                        .collect(Collectors.joining(","))).append(")\n");
            }
            script.append("DISTRIBUTED BY HASH(");
            if (pks.size() > 0) {
                script.append(pks.stream()
                        .map((pk) -> this.colEscapeChar() + pk.getName() + this.colEscapeChar())
                        .collect(Collectors.joining(",")));
            } else {
                List<ISelectedTab.ColMeta> cols = this.getCols();
                Optional<ISelectedTab.ColMeta> firstCol = cols.stream().findFirst();
                if (firstCol.isPresent()) {
                    script.append(firstCol.get().getName());
                } else {
                    throw new IllegalStateException("can not find table:" + getCreateTableName() + " any cols");
                }
            }
            script.append(")\n");
            script.append("BUCKETS 10\n");
            script.append("PROPERTIES(\"replication_num\" = \"1\")");
        }

        @Override
        protected ColWrapper createColWrapper(ISelectedTab.ColMeta c) {
            return new ColWrapper(c) {
                @Override
                public String getMapperType() {
                    return convertType(this.meta).token;
                }

                @Override
                protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                    if (this.meta.isPk()) {
                        ddlScript.append(" NOT NULL");
                    }
                }
            };
        }

        protected DorisType convertType(ISelectedTab.ColMeta col) {
            DataType type = col.getType();
            return type.accept(columnTokenRecognise);
        }
    }

    public static class DorisType implements Serializable {
        public final DataType type;
        final String token;

        public DorisType(DataType type, String token) {
            this.type = type;
            this.token = token;
        }
    }

    public static final DataType.TypeVisitor<DorisType> columnTokenRecognise
            = new DataType.TypeVisitor<DorisType>() {
        @Override
        public DorisType bigInt(DataType type) {
            return new DorisType(type, "BIGINT");
        }

        @Override
        public DorisType doubleType(DataType type) {
            return new DorisType(type, "DOUBLE");
        }

        @Override
        public DorisType dateType(DataType type) {
            return new DorisType(type, "DATE");
        }

        @Override
        public DorisType timestampType(DataType type) {
            return new DorisType(type, "DATETIME");
        }

        @Override
        public DorisType bitType(DataType type) {
            return new DorisType(type, "TINYINT");
        }

        @Override
        public DorisType blobType(DataType type) {
            return varcharType(type);
        }

        @Override
        public DorisType varcharType(DataType type) {
            return new DorisType(type, "VARCHAR(" + Math.min(type.columnSize, 65000) + ")");
        }

        @Override
        public DorisType intType(DataType type) {
            return new DorisType(type, "INT");
        }

        @Override
        public DorisType floatType(DataType type) {
            return new DorisType(type, "FLOAT");
        }

        @Override
        public DorisType decimalType(DataType type) {
            return new DorisType(type, "DECIMAL(" + type.columnSize + "," + (type.getDecimalDigits() != null ? type.getDecimalDigits() : 0) + ")");
        }
    };

//    public static DataType.TypeVisitor<String> getDorisColumnTokenRecognise() {
//        return columnTokenRecognise;
//    }

    protected static abstract class BaseDescriptor extends RdbmsWriterDescriptor {
        public BaseDescriptor() {
            super();
        }

        @Override
        protected int getMaxBatchSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        public boolean validateLoadProps(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            try {
                JSONObject props = JSON.parseObject(value);
                boolean valid = true;
                if (StringUtils.isEmpty(props.getString(Separator.COL_SEPARATOR))) {
                    msgHandler.addFieldError(context, fieldName, "必须包含key:'" + Separator.COL_SEPARATOR + "'");
                    valid = false;
                }
                if (StringUtils.isEmpty(props.getString(Separator.ROW_DELIMITER))) {
                    msgHandler.addFieldError(context, fieldName, "必须包含key:'" + Separator.ROW_DELIMITER + "'");
                    valid = false;
                }

                return valid;
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "错误的JSON格式：" + e.getMessage());
                return false;
            }
        }

        @Override
        protected abstract EndType getEndType();
//        {
//            return EndType.StarRocks;
//        }

//        @Override
//        public String getDisplayName() {
//            return DorisSourceFactory.NAME_DORIS;
//        }
    }
}
