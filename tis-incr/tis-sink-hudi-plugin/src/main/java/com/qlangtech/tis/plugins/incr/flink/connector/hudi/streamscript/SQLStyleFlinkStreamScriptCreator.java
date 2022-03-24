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

package com.qlangtech.tis.plugins.incr.flink.connector.hudi.streamscript;

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import com.qlangtech.tis.plugin.datax.hudi.HudiWriteTabType;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.HudiSinkFactory;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-24 11:02
 **/
public class SQLStyleFlinkStreamScriptCreator extends BasicFlinkStreamScriptCreator {


    public SQLStyleFlinkStreamScriptCreator(HudiSinkFactory hudiSinkFactory) {
        super(hudiSinkFactory);
    }


    @Override
    public String getFlinkStreamGenerateTemplateFileName() {
        return TEMPLATE_FLINK_TABLE_HANDLE_SCALA;
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return new HudiStreamTemplateData(mergeData);
    }

    public class HudiStreamTemplateData extends AdapterStreamTemplateData {
        public HudiStreamTemplateData(IStreamTemplateData data) {
            super(data);
        }

        public String getSourceTable(String tableName) {
            return tableName + IStreamTemplateData.KEY_STREAM_SOURCE_TABLE_SUFFIX;
        }

        public List<HdfsColMeta> getCols(String tableName) {
            Pair<HudiSelectedTab, HudiTableMeta> tableMeta = hudiSinkFactory.getTableMeta(tableName);
            return tableMeta.getRight().colMetas;
        }

        public StringBuffer getSinkFlinkTableDDL(String tableName) {
            DataXHudiWriter dataXWriter = HudiSinkFactory.getDataXHudiWriter(hudiSinkFactory);
            Pair<HudiSelectedTab, HudiTableMeta> tabMetaPair = hudiSinkFactory.getTableMeta(tableName);
            final HudiTableMeta tabMeta = tabMetaPair.getRight();
            HudiSelectedTab tab = tabMetaPair.getLeft();
            /**
             *
             * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sql/create/#create-table
             * @return
             */
            CreateTableSqlBuilder flinkTableDdlBuilder
                    = new CreateTableSqlBuilder(IDataxProcessor.TableMap.create(tableName, tabMeta.colMetas)) {
                @Override
                protected ColWrapper createColWrapper(ISelectedTab.ColMeta c) {
                    return new ColWrapper(c) {
                        @Override
                        public String getMapperType() {
                            return convertType(meta);
                        }

                        @Override
                        protected void appendExtraConstraint(BlockScriptBuffer ddlScript) {
                            // super.appendExtraConstraint(ddlScript);
                            // appendExtraConstraint
                            Optional<ColWrapper> f
                                    = pks.stream().filter((pk) -> pk.getName().equals(meta.getName())).findFirst();
                            if (f.isPresent()) {
                                ddlScript.append(" PRIMARY KEY NOT ENFORCED");
                            }
                        }
                    };
                }

                @Override
                protected void appendExtraColDef(List<ColWrapper> pks) {
                    // super.appendExtraColDef(pks);
                    // this.script.append(this.colEscapeChar()).append()
                    //String pt = tabMeta.get
                    //`partition` VARCHAR(20)

                    Objects.requireNonNull(tab.partition, "partition can not be null")
                            .addPartitionsOnSQLDDL(Collections.singletonList(dataXWriter.partitionedBy), this);

                    //                    this.script.appendLine("\t,");
                    //                    appendColName(dataXWriter.partitionedBy);
                    //                    this.script
                    //                            .append("VARCHAR(30)")
                    //                            .returnLine();
                }

                @Override
                protected void appendTabMeta(List<ColWrapper> pks) {
                    //                        with (
                    //                                'connector' = 'hudi',
                    //                                'path' = '$HUDI_DEMO/t2', -- $HUDI_DEMO 替换成的绝对路径
                    //                        'table.type' = 'MERGE_ON_READ',
                    //                                'write.bucket_assign.tasks' = '2',
                    //                                'write.tasks' = '2',
                    //                                'hive_sync.enable' = 'true',
                    //                                'hive_sync.mode' = 'hms',
                    //                                'hive_sync.metastore.uris' = 'thrift://ip:9083' -- ip 替换成 HMS 的地址
                    //                       );
                    if (tab.partition.isSupportPartition()) {
                        this.script.block("PARTITIONED BY", (sub) -> {
                            // (`partition`)
                            sub.appendLine("`" + dataXWriter.partitionedBy + "`");
                        });
                    }
                    IHiveConnGetter hiveCfg = dataXWriter.getHiveConnMeta();
                    if (StringUtils.isEmpty(hudiSinkFactory.getDataXName())) {
                        throw new IllegalStateException("prop of dataXName can not be empty");
                    }
                    this.script.block(!tab.partition.isSupportPartition(), "WITH", (sub) -> {
                        sub.appendLine("'" + DataxUtils.DATAX_NAME + "' = '" + hudiSinkFactory.getDataXName() + "',");
                        sub.appendLine("'connector' = 'hudi',");
                        sub.appendLine("'path' = '" + HudiTableMeta.getHudiDataDir(
                                dataXWriter.getFs().getFileSystem(), tableName, hudiSinkFactory.dumpTimeStamp, dataXWriter.getHiveConnMeta()) + "',");
                        sub.appendLine("'table.type' = '" + tabMeta.getHudiTabType().getValue() + "',");

                        //                        IPath fsSourceSchemaPath = HudiTableMeta.createFsSourceSchema(
                        //                                dataXWriter.getFs().getFileSystem(), dataXWriter.getHiveConnMeta()
                        //                                , tableName, dumpTimeStamp, getTableMeta(tableName));
                        // FlinkOptions
                        //  sub.appendLine("'source.avro-schema.path' = '" + String.valueOf(fsSourceSchemaPath) + "' ,");

                        if (tabMeta.getHudiTabType() == HudiWriteTabType.MOR) {
                            sub.appendLine("'read.streaming.enabled' = 'true',");
                            sub.appendLine("'read.streaming.check-interval' = '4',");
                        }

                        sub.appendLine("'hive_sync.enable' = 'true',");
                        sub.appendLine("'hive_sync.table' = '" + tableName + "',");
                        sub.appendLine("'hive_sync.database' = '" + hiveCfg.getDbName() + "',");
                        sub.appendLine("'hive_sync.mode'   = '" + HudiSinkFactory.HIVE_SYNC_MODE + "',");
                        sub.appendLine("'hive_sync.metastore.uris' = '" + hiveCfg.getMetaStoreUrls() + "'");

                    });
                }
            };
            return flinkTableDdlBuilder.build();
        }
    }

    private String convertType(ISelectedTab.ColMeta col) {
        // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/
        return col.getType().accept(new DataType.TypeVisitor<String>() {
            @Override
            public String bigInt(DataType type) {
                return "BIGINT";
            }

            @Override
            public String doubleType(DataType type) {
                return "DOUBLE";
            }

            @Override
            public String decimalType(DataType type) {
                // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/#decimal
                return "DECIMAL(" + type.columnSize + ", " + type.getDecimalDigits() + ")";
            }

            @Override
            public String dateType(DataType type) {
                // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/#date-and-time
                return "DATE";
            }

            @Override
            public String timestampType(DataType type) {
                // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/#timestamp
                return "TIMESTAMP(3)";
            }

            @Override
            public String bitType(DataType type) {
                return "BINARY(" + type.columnSize + ")";
            }

            @Override
            public String blobType(DataType type) {
                return "BYTES";
            }

            @Override
            public String varcharType(DataType type) {
                return "VARCHAR(" + type.columnSize + ")";
            }
        });
    }
}
