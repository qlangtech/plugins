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

package com.qlangtech.tis.plugin.datax.doris;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataTypeMeta;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
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
public abstract class BasicDorisWriter extends BasicDataXRdbmsWriter<DorisSourceFactory> {

    @FormField(ordinal = 10, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String loadProps;
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxBatchRows;


    /**
     * 提增量处理模块使用
     *
     * @return
     */
    public abstract Separator getSeparator();


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


    @Override
    public final CreateTableSqlBuilder.CreateDDL generateCreateDDL(IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        //        if (!this.autoCreateTable) {
        //            return null;
        //        }
        // https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Types/DATETIMEV2/
        // https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE
        // https://docs.starrocks.io/zh-cn/2.4/sql-reference/sql-statements/data-definition/CREATE%20TABLE
        final BasicCreateTableSqlBuilder createTableSqlBuilder = createSQLDDLBuilder(tableMapper, transformers);

        return createTableSqlBuilder.build();
    }

    protected abstract BasicCreateTableSqlBuilder createSQLDDLBuilder(IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers);


    protected static abstract class BasicCreateTableSqlBuilder extends CreateTableSqlBuilder {
        private final ISelectedTab dorisTab;
        private final List<String> primaryKeys;
        private final DataType.TypeVisitor<DorisType> columnTokenRecognise;

        public BasicCreateTableSqlBuilder(IDataxProcessor.TableMap tableMapper
                , DataSourceMeta dsMeta
                , DataType.TypeVisitor<DorisType> columnTokenRecognise
                , Optional<RecordTransformerRules> transformers) {
            super(tableMapper, dsMeta, transformers);
            // (DorisSelectedTab)
            this.dorisTab = tableMapper.getSourceTab();
            this.primaryKeys = this.dorisTab.getPrimaryKeys();
            this.columnTokenRecognise = columnTokenRecognise;
        }

        @Override
        protected void appendExtraColDef(List<String> pks) {

        }

        protected abstract String getUniqueKeyToken();


        @Override
        protected List<ColWrapper> preProcessCols(List<String> pks, List<IColMetaGetter> cols) {
            // 将主键排在最前面
            List<ColWrapper> result = Lists.newArrayList();
            for (String pk : primaryKeys) {
                for (IColMetaGetter c : cols) {
                    if (pk.equalsIgnoreCase(c.getName())) {
                        result.add(createColWrapper(c));
                    }
                }
            }
//                    Lists.newArrayList(cols.stream().filter((c) -> primaryKeys.contains(c.getName())).map((c) -> createColWrapper(c)).collect(Collectors.toList()));
            cols.stream().filter((c) -> !primaryKeys.contains(c.getName())).forEach((c) -> {
                result.add(createColWrapper(c));
            });
            return result;
        }

        @Override
        protected void appendTabMeta(List<String> pks) {
            script.append(" ENGINE=olap").append("\n");
            if (pks.size() > 0) {
                script.append(getUniqueKeyToken() + "(").append(pks.stream().map((pk) -> wrapWithEscape(pk)).collect(Collectors.joining(","))).append(")\n");
            }
            script.append("DISTRIBUTED BY HASH(");
            if (pks.size() > 0) {
                script.append(pks.stream().map((pk) -> wrapWithEscape(pk)).collect(Collectors.joining(",")));
            } else {
                List<IColMetaGetter> cols = this.getCols();
                Optional<IColMetaGetter> firstCol = cols.stream().findFirst();
                if (firstCol.isPresent()) {
                    script.append(firstCol.get().getName());
                } else {
                    throw new IllegalStateException("can not find table:" + getCreateTableName() + " any cols");
                }
            }
            script.append(")\n");
            script.append("BUCKETS 10\n");
            StringBuffer seqBuffer = new StringBuffer();
            if (dorisTab instanceof DorisSelectedTab) {
                seqBuffer = ((DorisSelectedTab) dorisTab).seqKey.createDDLScript(this);
            }

            script.append("PROPERTIES(\"replication_num\" = \"1\" " + seqBuffer + " )");


        }

        @Override
        protected ColWrapper createColWrapper(IColMetaGetter c) {
            return new ColWrapper(c) {
                @Override
                public String getMapperType() {
                    return convertType(this.meta).token;
                }

                @Override
                protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
//                    if (this.meta.isPk()) {
//                        ddlScript.append(" NOT NULL");
//                    }

                    if (primaryKeys.contains(this.meta.getName())) {
                        ddlScript.append(" NOT NULL");
                    }
                }
            };
        }

        protected DorisType convertType(IColMetaGetter col) {
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

    protected static class ColumnTokenRecognise implements DataType.TypeVisitor<DorisType> {
        @Override
        public DorisType tinyIntType(DataType dataType) {
            return new DorisType(dataType, "TINYINT");
        }

        @Override
        public DorisType smallIntType(DataType dataType) {
            return new DorisType(dataType, "SMALLINT");
        }

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
            // 原因：varchar(n) 再mysql中的n是字符数量，doris中的字节数量，所以如果在mysql中是varchar（n）在doris中varchar(3*N)
            // 三倍，doris中是按照utf-8字节数计算的
            int colSize = type.getColumnSize();
            if (colSize < 1 || colSize > 1500) {
                // https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Types/STRING/
                return new DorisType(type, "STRING");
            }
            return new DorisType(type, "VARCHAR(" + Math.min(colSize * 3, 65000) + ")");
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
        public final DorisType decimalType(DataType type) {
            // doris or starRocks precision 不能超过超过半27
            int precision = type.getColumnSize();
            if (precision < 1 || precision > DataTypeMeta.DEFAULT_DECIMAL_PRECISION) {
                precision = DataTypeMeta.DEFAULT_DECIMAL_PRECISION;
            }

            return new DorisType(type,
                    getDecimalToken() + "(" + precision + "," + (type.getDecimalDigits() != null ?
                            type.getDecimalDigits() : 0) + ")");
        }

        protected String getDecimalToken() {
            return "DECIMAL";
        }

    }


    protected static abstract class BaseDescriptor extends RdbmsWriterDescriptor {
        public BaseDescriptor() {
            super();
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        protected int getMaxBatchSize() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }


        public boolean validateMaxBatchRows(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                            String value) {
            int batchRows = Integer.parseInt(value);
            final int MaxBatchRows = 10000;
            if (batchRows < MaxBatchRows) {
                msgHandler.addFieldError(context, fieldName, "批次提交记录数不能小于:'" + MaxBatchRows + "'");
                return false;
            }
            return true;
        }

        public boolean validateLoadProps(IFieldErrorHandler msgHandler, Context context, String fieldName,
                                         String value) {
            try {
                JSONObject props = JSON.parseObject(value);
                boolean valid = true;
                if (StringUtils.isEmpty(props.getString(getColSeparatorKey()))) {
                    msgHandler.addFieldError(context, fieldName, "必须包含key:'" + getColSeparatorKey() + "'");
                    valid = false;
                }
                if (StringUtils.isEmpty(props.getString(getRowDelimiterKey()))) {
                    msgHandler.addFieldError(context, fieldName, "必须包含key:'" + getRowDelimiterKey() + "'");
                    valid = false;
                }

                return valid;
            } catch (Exception e) {
                msgHandler.addFieldError(context, fieldName, "错误的JSON格式：" + e.getMessage());
                return false;
            }
        }


        protected abstract String getRowDelimiterKey();

        protected abstract String getColSeparatorKey();
    }
}
