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
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.AbstractCreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.doris.BasicDorisWriter.DorisType;
import com.qlangtech.tis.plugin.datax.doris.DorisAutoCreateTable.DorisColWrapper;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.datax.doris.DataXDorisWriter.columnTokenRecognise;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-16 16:14
 **/
public class DorisAutoCreateTable extends ParamsAutoCreateTable<DorisColWrapper> {

    @FormField(ordinal = 1, validate = {Validator.require})
    public CreateTable createTableModel;
    /**
     * doris副本数目
     */
    @FormField(ordinal = 4, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer replicationNum;
    /**
     * doris表分桶数
     */
    @FormField(ordinal = 6, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer bucketsNum;


    public Integer getReplicationNum() {
        return replicationNum;
    }


    public Integer getBucketsNum() {
        return bucketsNum;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    private static class DorisCreateTableName extends CreateTableSqlBuilder.CreateTableName {
        public DorisCreateTableName(String tabName, AbstractCreateTableSqlBuilder sqlBuilder) {
            super(tabName, sqlBuilder);
        }

        @Override
        public String createTablePredicate() {
            return super.createTablePredicate() + " IF NOT EXISTS";
        }
    }

    @Override
    public final CreateTableSqlBuilder<DorisColWrapper> createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {

        DataXDorisWriter dorisWriter = (DataXDorisWriter) rdbmsWriter;

        return new BasicCreateTableSqlBuilder(this, tableMapper
                , dorisWriter.getDataSourceFactory(), columnTokenRecognise, transformers) {
            @Override
            protected String getUniqueKeyToken() {
                return Objects.requireNonNull(createTableModel, "createTableModel").getKeyToken();
            }

            @Override
            public CreateTableName getCreateTableName() {
                //return super.getCreateTableName();
                return new DorisCreateTableName(this.targetTableName, this);
            }

            @Override
            protected DorisColWrapper createColWrapper(IColMetaGetter col) {
                return new DorisColWrapper(sourceColMetaGetter, tableMapper
                        , DorisAutoCreateTable.this, col, this.pks, columnTokenRecognise);
            }
        };
    }

    @TISExtension
    public static final class DftDesc extends ParamsAutoCreateTable.DftDesc {
        public DftDesc() {
            super();
        }

        public boolean validateBucketsNum(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int bucketsNum = Integer.parseInt(val);
            if (bucketsNum < 1) {
                msgHandler.addFieldError(context, fieldName, "不能小于1");
                return false;
            }
            return true;
        }

        public boolean validateReplicationNum(
                IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {

            int replicationNum = Integer.parseInt(val);
            if (replicationNum < 1) {
                msgHandler.addFieldError(context, fieldName, "不能小于1");
                return false;
            }

            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Doris;
        }
    }

    protected static abstract class BasicCreateTableSqlBuilder extends CreateTableSqlBuilder<DorisColWrapper> {
        private final ISelectedTab dorisTab;
        private final List<DorisColWrapper> primaryKeys;
        private final DataType.TypeVisitor<DorisType> columnTokenRecognise;
        private final DorisAutoCreateTable autoCreateTable;
        private static final Comparator<DorisColWrapper> pkSortCompare = new Comparator<DorisColWrapper>() {
            @Override
            public int compare(DorisColWrapper col1, DorisColWrapper col2) {
                DorisType type1 = col1.dorisType;
                DorisType type2 = col2.dorisType;
                return (type1.dateType ? 1 : 0) - (type2.dateType ? 1 : 0);
            }
        };

        public BasicCreateTableSqlBuilder(DorisAutoCreateTable autoCreateTable, TableMap tableMapper
                , DataSourceMeta dsMeta
                , DataType.TypeVisitor<DorisType> columnTokenRecognise
                , Optional<RecordTransformerRules> transformers) {
            super(tableMapper, dsMeta, transformers);
            this.autoCreateTable = autoCreateTable;
            this.columnTokenRecognise = columnTokenRecognise;
            this.dorisTab = tableMapper.getSourceTab();
            this.primaryKeys = Lists.newArrayList();
            for (String pk : this.dorisTab.getPrimaryKeys()) {
                for (DorisColWrapper c : this.getCols()) {
                    if (pk.equalsIgnoreCase(c.getName())) {
                        // result.add(createColWrapper(c));
                        this.primaryKeys.add((c));
                    }
                }
            }
            this.primaryKeys.sort(pkSortCompare);


        }

        @Override
        protected void appendExtraColDef(List<String> pks) {

        }

        public boolean isPK(String colName) {
            return this.pks.contains(colName);
        }

        protected abstract String getUniqueKeyToken();

        @Override
        protected List<DorisColWrapper> preProcessCols(List<String> pks, List<DorisColWrapper> cols) {
            // 将主键排在最前面
            List<DorisColWrapper> result = Lists.newArrayList();
            result.addAll(primaryKeys);


            cols.stream().filter((c) -> !this.pks.contains(c.getName())).forEach((c) -> {
                result.add((c));
            });
            return result;
        }

        @Override
        protected void appendTabMeta(List<String> pks) {


            script.append(" ENGINE=olap").append("\n");
            if (pks.size() > 0) {
                script.append(getUniqueKeyToken() + "(")
                        .append(primaryKeys.stream().map((pk) -> wrapWithEscape(pk.getName())).collect(Collectors.joining(",")))
                        .append(")\n");
            }
            script.append("DISTRIBUTED BY HASH(");
            if (pks.size() > 0) {
                script.append(primaryKeys.stream().map((pk) -> wrapWithEscape(pk.getName())).collect(Collectors.joining(",")));
            } else {
                List<DorisColWrapper> cols = this.getCols();
                Optional<DorisColWrapper> firstCol = cols.stream().findFirst();
                if (firstCol.isPresent()) {
                    script.append(firstCol.get().getName());
                } else {
                    throw new IllegalStateException("can not find table:" + getCreateTableName() + " any cols");
                }
            }
            script.append(")\n");
            script.append("BUCKETS " + autoCreateTable.getBucketsNum() + "\n");
            StringBuffer seqBuffer = new StringBuffer();
            if (dorisTab instanceof DorisSelectedTab) {
                seqBuffer = ((DorisSelectedTab) dorisTab).seqKey.createDDLScript(this);
            }

            script.append("PROPERTIES(\"replication_num\" = \"" + autoCreateTable.getReplicationNum() + "\" " + seqBuffer + " )");


        }
    }

    protected static final class DorisColWrapper extends ColWrapper {
        protected DorisType dorisType;
        private final DataType.TypeVisitor<DorisType> columnTokenRecognise;
        private final AutoCreateTable autoCreateTable;
        private final SourceColMetaGetter sourceColMetaGetter;
        private final TableMap tableMapper;

        public DorisColWrapper(SourceColMetaGetter sourceColMetaGetter, TableMap tableMapper
                , AutoCreateTable autoCreateTable, IColMetaGetter meta
                , List<String> pks, DataType.TypeVisitor<DorisType> columnTokenRecognise) {
            super(meta, pks);
            this.columnTokenRecognise = Objects.requireNonNull(columnTokenRecognise, "columnTokenRecognise can not be null");
            this.dorisType = convertType(meta);
            this.autoCreateTable = Objects.requireNonNull(autoCreateTable, "autoCreateTable can not be null");
            this.sourceColMetaGetter = sourceColMetaGetter;
            this.tableMapper = Objects.requireNonNull(tableMapper, "tableMapper can not be null");
        }

        @Override
        public final String getMapperType() {
            return dorisType.token;
        }

        @Override
        protected final void appendExtraConstraint(BlockScriptBuffer ddlScript) {
            if (this.isPk()) {
                ddlScript.append(" NOT NULL");
            }
            // SourceColMetaGetter sourceColMetaGetter, TableMap tableMapper, ColWrapper colWrapper
            autoCreateTable.getAddComment().addStandardColComment(sourceColMetaGetter, tableMapper, this, ddlScript);
        }

        protected DorisType convertType(IColMetaGetter col) {

            final DorisType type = col.getType().accept((columnTokenRecognise));

            DorisType fixType = col.getType().accept(new DataType.TypeVisitor<DorisType>() {

                @Override
                public DorisType bigInt(DataType type) {
                    return null;
                }

                @Override
                public DorisType doubleType(DataType type) {
                    return null;
                }

                @Override
                public DorisType dateType(DataType type) {
                    return new DorisType(type, true, "DATEV2");
                }

                @Override
                public DorisType timestampType(DataType type) {
                    return new DorisType(type, true, "DATETIMEV2");
                }

                @Override
                public DorisType bitType(DataType type) {
                    return null;
                }

                @Override
                public DorisType blobType(DataType type) {
                    return null;
                }

                @Override
                public DorisType varcharType(DataType type) {
                    return null;
                }
            });
            return fixType != null ? fixType : type;
        }
    }
}
