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
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:19
 **/
public class OdpsAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {

    /**
     * https://help.aliyun.com/document_detail/159540.html?spm=a2c4g.11186623.0.0.6ae36f60bs6Lm2
     */
    public static DataType.TypeVisitor<String> typeTransfer = new DataType.TypeVisitor<String>() {
        @Override
        public String bigInt(DataType type) {
            return "BIGINT";
        }

        @Override
        public String decimalType(DataType type) {
            return "DECIMAL(" + type.getColumnSize() + "," + type.getDecimalDigits() + ")";
        }

        @Override
        public String intType(DataType type) {
            return "INT";
        }

        @Override
        public String tinyIntType(DataType dataType) {
            return "TINYINT";
        }

        @Override
        public String smallIntType(DataType dataType) {
            return "SMALLINT";
        }

        @Override
        public String boolType(DataType dataType) {
            return "BOOLEAN";
        }

        @Override
        public String floatType(DataType type) {
            return "FLOAT";
        }

        @Override
        public String doubleType(DataType type) {
            return "DOUBLE";
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
            return "STRING";
        }

        @Override
        public String blobType(DataType type) {
            return "BINARY";
        }

        @Override
        public String varcharType(DataType type) {
            return "STRING";
        }
    };

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        final DataXOdpsWriter odpsWriter = (DataXOdpsWriter) rdbmsWriter;
        final CreateTableSqlBuilder createTableSqlBuilder
                = new CreateTableSqlBuilder<>(tableMapper, odpsWriter.getDataSourceFactory(), transformers) {

            @Override
            protected String createTargetTableName(TableMap tableMapper) {
                return tableMapper.createTargetTableName(OdpsAutoCreateTable.this);
                //  return tableMapper.isFromEqualTo() ? appendTabPrefix(tableMapper.getFrom()) : tableMapper.getTo();
                // return appendTabPrefix(tableMapper.getTo());
            }

            @Override
            public CreateTableName getCreateTableName() {
                CreateTableName nameBuilder = super.getCreateTableName();
                // EXTERNAL
                nameBuilder.setCreateTablePredicate("CREATE TABLE IF NOT EXISTS");
                return nameBuilder;
            }

            @Override
            protected void appendTabMeta(List<String> pks) {

                // HdfsFormat fsFormat = parseFSFormat();
                script.appendLine("COMMENT 'tis_tmp_" + tableMapper.getTo()
                        + "' PARTITIONED BY(" + IDumpTable.PARTITION_PT + " string," + IDumpTable.PARTITION_PMOD + " string)   ");
                script.append("lifecycle " + Objects.requireNonNull(odpsWriter.lifecycle, "lifecycle can not be null"));

                // script.appendLine(fsFormat.getRowFormat());
                // script.appendLine("STORED AS " + fsFormat.getFileType().getType());

//                script.appendLine("LOCATION '").append(
//                        FSHistoryFileUtils.getJoinTableStorePath(fileSystem.getRootDir(), getDumpTab(tableMapper.getTo()))
//                ).append("'");
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c, this.pks) {
                    @Override
                    public String getMapperType() {
                        return c.getType().accept(typeTransfer);
                    }

                    @Override
                    protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                        OdpsAutoCreateTable.this.addComment.addStandardColComment(sourceColMetaGetter, tableMapper, this, ddlScript);
                    }
                };
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
            return EndType.AliyunODPS;
        }
    }
}
