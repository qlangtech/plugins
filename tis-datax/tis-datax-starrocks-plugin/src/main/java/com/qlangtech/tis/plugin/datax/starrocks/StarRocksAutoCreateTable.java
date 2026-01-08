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

package com.qlangtech.tis.plugin.datax.starrocks;

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
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.ds.BasicDataSourceFactory.isJSONColumnType;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:55
 **/
public class StarRocksAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {
    /**
     * StarRocks副本数目
     */
    @FormField(ordinal = 1, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer replicationNum;
    /**
     * StarRocks表分桶数
     */
    @FormField(ordinal = 2, advance = true, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer bucketsNum;

    public static final DataType.TypeVisitor<StarRocksType> columnTokenRecognise
            = new DataType.PartialTypeVisitor<StarRocksType>() {
        @Override
        public StarRocksType tinyIntType(DataType dataType) {
            return new StarRocksType(dataType, "TINYINT");
        }

        @Override
        public StarRocksType smallIntType(DataType dataType) {
            return new StarRocksType(dataType, "SMALLINT");
        }

        @Override
        public StarRocksType bigInt(DataType type) {
            return new StarRocksType(type, "BIGINT");
        }

        @Override
        public StarRocksType doubleType(DataType type) {
            return new StarRocksType(type, "DOUBLE");
        }

        @Override
        public StarRocksType dateType(DataType type) {
            return new StarRocksType(type, "DATE");
        }

        @Override
        public StarRocksType timestampType(DataType type) {
            return new StarRocksType(type, "DATETIME");
        }

        @Override
        public StarRocksType bitType(DataType type) {
            return new StarRocksType(type, "TINYINT");
        }

        @Override
        public StarRocksType blobType(DataType type) {
            return varcharType(type);
        }

        @Override
        public StarRocksType varcharType(DataType type) {
            // 原因：varchar(n) 再mysql中的n是字符数量，doris中的字节数量，所以如果在mysql中是varchar（n）在doris中varchar(3*N) 三倍，doris中是按照utf-8字节数计算的
            if (isJSONColumnType(type)) {
                return new StarRocksType(type, "JSON");
            }

            int colSize = type.getColumnSize();
            if (colSize < 1 || colSize > 1500) {
                // https://doris.apache.org/docs/1.2/sql-manual/sql-reference/Data-Types/STRING/
                return new StarRocksType(type, "STRING");
            }

            return new StarRocksType(type, "VARCHAR(" + Math.min(type.getColumnSize() * 3, 65000) + ")");
        }

        @Override
        public StarRocksType intType(DataType type) {
            return new StarRocksType(type, "INT");
        }

        @Override
        public StarRocksType floatType(DataType type) {
            return new StarRocksType(type, "FLOAT");
        }

        @Override
        public StarRocksType decimalType(DataType type) {
            // doris or starRocks precision 不能超过超过半27
            return new StarRocksType(type, "DECIMAL(" + Math.min(type.getColumnSize(), 27) + "," + (type.getDecimalDigits() != null ? type.getDecimalDigits() : 0) + ")");
        }
    };

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        DataXStarRocksWriter rocksWriter = (DataXStarRocksWriter) rdbmsWriter;

        return new BasicCreateTableSqlBuilder(sourceColMetaGetter, tableMapper, rocksWriter.getDataSourceFactory(), transformers) {
            @Override
            protected String getUniqueKeyToken() {
                return "PRIMARY KEY";
            }
        };
    }

    @TISExtension
    public static class Desc extends ParamsAutoCreateTable.DftDesc {
        public Desc() {
            super();
        }

        @Override
        public EndType getEndType() {
            return EndType.StarRocks;
        }
    }

    private static class StarRocksCreateTableName extends CreateTableSqlBuilder.CreateTableName {
        public StarRocksCreateTableName(String tabName, AbstractCreateTableSqlBuilder sqlBuilder) {
            super(tabName, sqlBuilder);
        }

        @Override
        public String createTablePredicate() {
            return super.createTablePredicate() + " IF NOT EXISTS";
        }
    }

    public abstract class BasicCreateTableSqlBuilder<T extends ColWrapper> extends CreateTableSqlBuilder<T> {
        private final SourceColMetaGetter sourceColMetaGetter;
        //private final AutoCreateTable autoCreateTable;
        private final TableMap tableMapper;

        public BasicCreateTableSqlBuilder(
                // AutoCreateTable autoCreateTable,
                SourceColMetaGetter sourceColMetaGetter, TableMap tableMapper
                , DataSourceMeta dsMeta, Optional<RecordTransformerRules> transformers) {
            super(tableMapper, dsMeta, transformers);
            //   this.autoCreateTable = autoCreateTable;
            this.sourceColMetaGetter = Objects.requireNonNull(sourceColMetaGetter, "sourceColMetaGetter can not be null");
            this.tableMapper = tableMapper;
        }

        @Override
        public CreateTableName getCreateTableName() {
            //return super.getCreateTableName();
            return new StarRocksCreateTableName(this.targetTableName, this);
        }

        @Override
        protected void appendExtraColDef(List<String> pks) {
//                if (pk != null) {
//                    script.append("  PRIMARY KEY (`").append(pk.getName()).append("`)").append("\n");
//                }
        }

        protected abstract String getUniqueKeyToken();


        @Override
        protected List<T> preProcessCols(List<String> pks, List<T> cols) {
            // 将主键排在最前面

            List<T> result = Lists.newArrayList();
            for (String pk : pks) {
                for (T c : cols) {
                    if (pk.equalsIgnoreCase(c.getName())) {
                        result.add(c);
                    }
                }
            }
            cols.stream().filter((c) -> !pks.contains(c.getName())).forEach((c) -> {
                result.add(c);
            });
            return result;
        }

        @Override
        protected void appendTabMeta(List<String> pks) {
            script.append(" ENGINE=olap").append("\n");
            if (pks.size() > 0) {
                script.append(getUniqueKeyToken() + "(").append(pks.stream()
                        .map((pk) -> wrapWithEscape(pk))
                        .collect(Collectors.joining(","))).append(")\n");
            }
            script.append("DISTRIBUTED BY HASH(");
            if (pks.size() > 0) {
                script.append(pks.stream()
                        .map((pk) -> wrapWithEscape(pk))
                        .collect(Collectors.joining(",")));
            } else {
                List<T> cols = this.getCols();
                Optional<T> firstCol = cols.stream().findFirst();
                if (firstCol.isPresent()) {
                    script.append(firstCol.get().getName());
                } else {
                    throw new IllegalStateException("can not find table:" + getCreateTableName() + " any cols");
                }
            }
            script.append(")\n");
            script.append("BUCKETS " + bucketsNum + "\n");
            script.append("PROPERTIES(\"replication_num\" = \"" + replicationNum + "\")");
        }

        @Override
        protected T createColWrapper(IColMetaGetter c) {
            return (T) new ColWrapper(c, this.pks) {
                @Override
                public String getMapperType() {
                    return convertType(getType()).token;
                }

                @Override
                protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                    if (this.isPk()) {
                        ddlScript.append(" NOT NULL");
                    }
                    addComment.addStandardColComment(sourceColMetaGetter, tableMapper, this, ddlScript);
                }
            };
        }

        protected StarRocksType convertType(DataType type) {
            return Objects.requireNonNull(type, "type can not be null")
                    .accept(columnTokenRecognise);
        }
    }

    public static class StarRocksType implements Serializable {
        public final DataType type;
        final String token;

        public StarRocksType(DataType type, String token) {
            this.type = type;
            this.token = token;
        }
    }
}

