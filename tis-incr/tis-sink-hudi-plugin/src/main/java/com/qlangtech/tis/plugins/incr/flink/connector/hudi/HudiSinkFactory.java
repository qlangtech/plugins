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

package com.qlangtech.tis.plugins.incr.flink.connector.hudi;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.GroovyShellEvaluate;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.order.center.IParamContext;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import com.qlangtech.tis.plugin.datax.hudi.HudiWriteTabType;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hudi.common.fs.IExtraHadoopFileSystemGetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-14 14:39
 **/
@Public
public class HudiSinkFactory extends TISSinkFactory implements IStreamTableCreator {
    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-Hudi-Sink";

    private static final Logger logger = LoggerFactory.getLogger(HudiSinkFactory.class);

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String catalog;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String database;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dumpTimeStamp;

    public static List<Option> getHistoryBatch() {

        HudiSinkFactory sink = (HudiSinkFactory) GroovyShellEvaluate.pluginThreadLocal.get();
        if (sink != null) {
            try {
                DataXHudiWriter dataXWriter = getDataXHudiWriter(sink);
                return HudiTableMeta.getHistoryBatchs(dataXWriter.getFs().getFileSystem(), dataXWriter.getHiveConnMeta());
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }

        return Lists.newArrayList(new Option(IParamContext.getCurrentTimeStamp()));

    }

    protected static DataXHudiWriter getDataXHudiWriter(HudiSinkFactory sink) {
        return (DataXHudiWriter) DataxWriter.getPluginStore(null, sink.dataXName).getPlugin();
    }

    @Override
    public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
        DataXHudiWriter hudiWriter = getDataXHudiWriter(this);

        if (!IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME.equals(hudiWriter.fsName)) {
            throw new IllegalStateException("fsName of hudiWriter must be equal to '"
                    + IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME + "', but now is " + hudiWriter.fsName);
        }

        return Collections.emptyMap();
    }

    /**
     * ------------------------------------------------------------------------------
     * start implements IStreamTableCreator
     * ------------------------------------------------------------------------------
     */
    private transient Map<String, HudiTableMeta> tableMetas = null;

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
            if (StringUtils.isEmpty(dataXName)) {
                throw new IllegalStateException("prop dataXName can not be null");
            }
        }

        public String getSourceTable(String tableName) {
            return tableName + IStreamTemplateData.KEY_STREAM_SOURCE_TABLE_SUFFIX;
        }

        public List<HdfsColMeta> getCols(String tableName) {
            HudiTableMeta tableMeta = getTableMeta(tableName);
            return tableMeta.colMetas;
        }

        public StringBuffer getSinkFlinkTableDDL(String tableName) {
            DataXHudiWriter dataXWriter = getDataXHudiWriter(HudiSinkFactory.this);
            HudiTableMeta tabMeta = getTableMeta(tableName);
            /**
             *
             * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sql/create/#create-table
             * @return
             */
            //@Override
//                public StringBuffer createFlinkTableDDL () {
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
                    this.script.appendLine("\t,");
                    appendColName(dataXWriter.partitionedBy);
                    this.script
                            .append("VARCHAR(30)")
                            .returnLine();
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
                    this.script.block("PARTITIONED BY", (sub) -> {
                        // (`partition`)
                        sub.appendLine("`" + dataXWriter.partitionedBy + "`");
                    });
                    IHiveConnGetter hiveCfg = dataXWriter.getHiveConnMeta();
                    if (StringUtils.isEmpty(dataXName)) {
                        throw new IllegalStateException("prop of dataXName can not be empty");
                    }
                    this.script.block(true, "WITH", (sub) -> {
                        sub.appendLine("'" + DataxUtils.DATAX_NAME + "' = '" + dataXName + "',");
                        sub.appendLine("'connector' = 'hudi',");
                        sub.appendLine("'path' = '" + tabMeta.getHudiDataDir(
                                dataXWriter.getFs().getFileSystem(), dumpTimeStamp, dataXWriter.getHiveConnMeta()) + "',");
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
                        sub.appendLine("'hive_sync.mode'   = 'hms',");
                        sub.appendLine("'hive_sync.metastore.uris' = '" + hiveCfg.getMetaStoreUrls() + "'");

                    });
                }
            };
            return flinkTableDdlBuilder.build();
        }
    }

    @Override
    public IStreamTableMeta getStreamTableMeta(final String tableName) {
        final HudiTableMeta tabMeta = getTableMeta(tableName);
        return new IStreamTableMeta() {
            @Override
            public List<HdfsColMeta> getColsMeta() {
                return tabMeta.colMetas;
            }
        };
    }

    private HudiTableMeta getTableMeta(String tableName) {
        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName can not empty");
        }
        if (tableMetas == null) {
            if (StringUtils.isEmpty(this.dataXName)) {
                throw new IllegalStateException("prop dataXName can not be null");
            }
            tableMetas = Maps.newHashMap();
            DataxProcessor dataXProcessor = DataxProcessor.load(null, this.dataXName);

            List<File> dataxCfgFile = dataXProcessor.getDataxCfgFileNames(null);
            Configuration cfg = null;
            Configuration paramCfg = null;
            String table = null;
            HudiTableMeta tableMeta = null;
            for (File f : dataxCfgFile) {
                cfg = Configuration.from(f);
                paramCfg = cfg.getConfiguration("job.content[0].writer.parameter");
                if (paramCfg == null) {
                    throw new NullPointerException("paramCfg can not be null,relevant path:" + f.getAbsolutePath());
                }
                table = paramCfg.getString("fileName");
                if (StringUtils.isEmpty(table)) {
                    throw new IllegalStateException("table can not be null:" + paramCfg.toJSON());
                }
                tableMeta = new HudiTableMeta(paramCfg);
                tableMetas.put(table, tableMeta);
            }
        }

        final HudiTableMeta tabMeta = tableMetas.get(tableName);
        if (tabMeta == null || tabMeta.isColsEmpty()) {
            throw new IllegalStateException("table:" + tableName
                    + " relevant colMetas can not be null,exist tables:"
                    + tableMetas.keySet().stream().collect(Collectors.joining(",")));
        }
        return tabMeta;
    }

    private String convertType(ISelectedTab.ColMeta col) {
        // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/types/
        return col.getType().accept(new DataType.TypeVisitor<String>() {
            @Override
            public String longType(DataType type) {
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

    /**
     * ------------------------------------------------------------------------------
     * End implements IStreamTableCreator
     * ------------------------------------------------------------------------------
     */

    @TISExtension
    public static class DefaultSinkFunctionDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            if (!DataXHudiWriter.HUDI_FILESYSTEM_NAME.equals(IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME)) {
                throw new IllegalStateException("DataXHudiWriter.HUDI_FILESYSTEM_NAME:" + DataXHudiWriter.HUDI_FILESYSTEM_NAME
                        + ",IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME:"
                        + IExtraHadoopFileSystemGetter.HUDI_FILESYSTEM_NAME + " must be equal");
            }
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected IDataXPluginMeta.EndType getTargetType() {
            return IDataXPluginMeta.EndType.Hudi;
        }
    }
}
