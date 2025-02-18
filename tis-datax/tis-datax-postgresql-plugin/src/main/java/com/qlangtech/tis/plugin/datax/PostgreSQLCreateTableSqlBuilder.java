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
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTableColCommentSwitch;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;
import com.qlangtech.tis.plugin.ds.postgresql.PGLikeDataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-10-11 12:41
 **/
public class PostgreSQLCreateTableSqlBuilder extends CreateTableSqlBuilder<ColWrapper> {
    private final boolean multiPk;
    private final PGLikeDataSourceFactory ds;
    private final TableMap tableMapper;
    private final AutoCreateTableColCommentSwitch colCommentAdd;
    private SourceColMetaGetter sourceColMetaGetter;

    public PostgreSQLCreateTableSqlBuilder(
            AutoCreateTableColCommentSwitch colCommentAdd, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, PGLikeDataSourceFactory dsMeta, Optional<RecordTransformerRules> transformers) {
        super(tableMapper, dsMeta, transformers);
        this.multiPk = this.pks.size() > 1;
        this.ds = dsMeta;
        this.tableMapper = tableMapper;
        this.colCommentAdd = Objects.requireNonNull(colCommentAdd);
        this.sourceColMetaGetter = Objects.requireNonNull(sourceColMetaGetter);
    }

    @Override
    public CreateTableName getCreateTableName() {
        return new CreateTableName(ds.tabSchema, tableMapper.getTo(), this);
    }

    @Override
    protected void appendExtraColDef(List<String> pks) {
        if (multiPk) {
            this.script.append(", CONSTRAINT ").append("uk_" + tableMapper.getTo() + "_unique_" + pks.stream().map((c) -> c).collect(Collectors.joining("_")))
                    .append(" UNIQUE(")
                    .append(pks.stream().map((c) -> c).collect(Collectors.joining(","))).append(")");
        }
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

    @Override
    protected void appendTabMeta(List<String> pks) {
        super.appendTabMeta(pks);
        this.colCommentAdd.addOracleLikeColComment(this, sourceColMetaGetter, tableMapper, script);
    }

    /**
     * https://www.runoob.com/mysql/mysql-data-types.html
     *
     * @param type
     * @return
     */
    private String convertType(DataType type, boolean isPk) {

        String colType = Objects.requireNonNull(type, "type can not be null")
                .accept(new DataType.TypeVisitor<String>() {
                    @Override
                    public String bigInt(DataType type) {
                        return "BIGINT";
                    }

                    @Override
                    public String doubleType(DataType type) {
                        return "FLOAT8";
                    }

                    @Override
                    public String dateType(DataType type) {
                        return "DATE";
                    }

                    @Override
                    public String timestampType(DataType type) {
                        return "TIMESTAMP";
                    }

                    @Override
                    public String bitType(DataType type) {
                        return "BIT";
                    }

                    @Override
                    public String blobType(DataType type) {
                        return "BYTEA";
                    }

                    @Override
                    public String varcharType(DataType type) {
                        if (type.getColumnSize() > Short.MAX_VALUE) {
                            return "TEXT";
                        }
                        return "VARCHAR(" + type.getColumnSize() + ")";
                    }

                    @Override
                    public String intType(DataType type) {
                        return "INTEGER";
                    }

                    @Override
                    public String floatType(DataType type) {
                        return "FLOAT4";
                    }

                    @Override
                    public String decimalType(DataType type) {
                        if (type.getColumnSize() > 0) {
                            return "DECIMAL(" + Math.min(type.getColumnSize(), 38) + "," + type.getDecimalDigits() + ")";
                        } else {
                            return "DECIMAL";
                        }
                    }

                    @Override
                    public String timeType(DataType type) {
                        return "TIME";
                    }

                    @Override
                    public String tinyIntType(DataType dataType) {
                        return smallIntType(dataType);
                    }

                    @Override
                    public String smallIntType(DataType dataType) {
                        return "SMALLINT";
                    }
                });

        return colType + ((!multiPk && isPk) ? " PRIMARY KEY" : StringUtils.EMPTY);
    }
}
