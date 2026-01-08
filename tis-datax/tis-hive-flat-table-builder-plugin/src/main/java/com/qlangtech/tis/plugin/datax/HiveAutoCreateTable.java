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
import com.qlangtech.tis.fs.FSHistoryFileUtils;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.hive.HdfsFormat;
import com.qlangtech.tis.hive.HiveColumn;
import com.qlangtech.tis.hive.Hiveserver2DataSourceFactory;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-17 10:46
 **/
public class HiveAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {

    private final Optional<String> tabProperties;


    public HiveAutoCreateTable() {
        this(Optional.empty());
    }

    public HiveAutoCreateTable(Optional<String> tabProperties) {
        this.tabProperties = tabProperties;
    }

    @Override
    public CreateTableSqlBuilder createSQLDDLBuilder(
            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
        DataXHiveWriter hiveWriter = (DataXHiveWriter) rdbmsWriter;
        final Hiveserver2DataSourceFactory dsFactory = hiveWriter.getDataSourceFactory();
        final ITISFileSystem fileSystem = hiveWriter.getFs().getFileSystem();
        final CreateTableSqlBuilder createTableSqlBuilder
                = new CreateTableSqlBuilder<ColWrapper>(tableMapper, hiveWriter.getDataSourceFactory(), transformers) {
//            @Override
//            protected String createTargetTableName(TableMap tableMapper) {
//                // return appendTabPrefix(tableMapper.getTo());
//                return tableMapper.createTargetTableName(HiveAutoCreateTable.this);
//            }

            @Override
            protected String createTargetTableName(TableMap tableMapper) {
                //  return super.createTargetTableName(tableMapper, autoCreateTable);

                return tableMapper.createTargetTableName(HiveAutoCreateTable.this);
            }

            @Override
            public CreateTableName getCreateTableName() {
                CreateTableName nameBuilder = super.getCreateTableName();
                nameBuilder.setCreateTablePredicate("CREATE EXTERNAL TABLE IF NOT EXISTS");
                return nameBuilder;
            }

            protected void appendTabMeta(List<String> pks) {

                HdfsFormat fsFormat = parseFSFormat();

                script.appendLine("COMMENT 'tis_tmp_" + targetTableName
                        + "' PARTITIONED BY(" + IDumpTable.PARTITION_PT + " string," + IDumpTable.PARTITION_PMOD + " string)   ");
                script.appendLine(fsFormat.getRowFormat());
                script.appendLine("STORED AS " + fsFormat.getFileType().getType());

                //  hiveWriter.getDumpTab(targetTableName)
                script.appendLine("LOCATION '").append(
                        FSHistoryFileUtils.getJoinTableStorePath(fileSystem.getRootDir()
                                , dsFactory.getSubTablePath(EntityName.parse(targetTableName)))
                ).append("'");

                tabProperties.ifPresent((tabProps) -> {
                    script.appendLine("TBLPROPERTIES (" + tabProps + ")");
                });
            }

            private HdfsFormat parseFSFormat() {

                HdfsFormat fsFormat = new HdfsFormat();

                fsFormat.setFieldDelimiter(String.valueOf(hiveWriter.fileType.getFieldDelimiter()));
                //  (String) TisDataXHiveWriter.jobFileType.get(this)
                fsFormat.setFileType(hiveWriter.fileType.getType());
                return fsFormat;
            }

            @Override
            protected ColWrapper createColWrapper(IColMetaGetter c) {
                return new ColWrapper(c, this.pks) {
                    @Override
                    public String getMapperType() {
                        String hiveType = c.getType().accept(HiveColumn.hiveTypeVisitor);
                        return hiveType;// StringUtils.defaultIfEmpty(fixType, );
                    }

                    @Override
                    protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                        addComment.addStandardColComment(sourceColMetaGetter, tableMapper, this, ddlScript);
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
            return EndType.HiveMetaStore;
        }
    }
}