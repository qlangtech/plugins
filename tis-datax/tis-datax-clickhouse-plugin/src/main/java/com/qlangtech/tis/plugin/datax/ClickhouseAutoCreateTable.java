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

import com.alibaba.datax.common.ck.ClickHouseCommon;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:15
 **/
public class ClickhouseAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {


    @Override
    public CreateTableSqlBuilder<ColWrapper> createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        BasicDataXRdbmsWriter dataXWriter = (BasicDataXRdbmsWriter) rdbmsWriter;
        final CreateTableSqlBuilder createTableSqlBuilder
                = new CreateTableSqlBuilder<>(tableMapper, dataXWriter.getDataSourceFactory(), transformers) {
            @Override
            protected void appendExtraColDef(List<String> pks) {
                script.append("   ," + wrapWithEscape(ClickHouseCommon.KEY_CLICKHOUSE_CK) + " Int8 DEFAULT 1").append("\n");
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c, this.pks) {
                    @Override
                    public String getMapperType() {
                        return convertType(this.getType());
                    }

                    @Override
                    protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                        ClickhouseAutoCreateTable.this.addComment.addStandardColComment(sourceColMetaGetter, tableMapper, this, ddlScript);
                    }
                };
            }

            @Override
            protected void appendTabMeta(List<String> pk) {
                script.append(" ENGINE = CollapsingMergeTree(" + ClickHouseCommon.KEY_CLICKHOUSE_CK + ")").append("\n");
                if (CollectionUtils.isNotEmpty(pk)) {
                    script.append(" ORDER BY (").append(pk.stream().map((p) -> this.wrapWithEscape(p)).collect(Collectors.joining(","))).append(")\n");
                }
                script.append(" SETTINGS index_granularity = 8192");
            }

            private String convertType(DataType type) {

                switch (Objects.requireNonNull(type, "type can not be null")
                        .getJdbcType()) {
                    case INTEGER:
                    case TINYINT:
                    case SMALLINT:
                        return "Int32";
                    case BIGINT:
                        return "Int64";
                    case FLOAT:
                        return "Float32";
                    case DOUBLE:
                    case DECIMAL:
                        return "Float64";
                    case DATE:
                        return "Date";
                    case TIME:
                    case TIMESTAMP:
                        return "DateTime";
                    case BIT:
                    case BOOLEAN:
                        return "UInt8";
                    case BLOB:
                    case BINARY:
                    case LONGVARBINARY:
                    case VARBINARY:
                    default:
                        return "String";
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
            return EndType.Clickhouse;
        }
    }
}
