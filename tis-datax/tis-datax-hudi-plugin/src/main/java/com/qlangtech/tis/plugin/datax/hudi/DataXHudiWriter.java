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

package com.qlangtech.tis.plugin.datax.hudi;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.hive.IHiveConn;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableCreator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicFSWriter;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.DataXHdfsWriter;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-21 13:02
 **/
@Public
public class DataXHudiWriter extends BasicFSWriter implements KeyedPluginStore.IPluginKeyAware, IHiveConn, IStreamTableCreator {
    public static final String DATAX_NAME = "Hudi";

    public static final String KEY_FIELD_NAME_SPARK_CONN = "sparkConn";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String sparkConn;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String hiveConn;

    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public String tabType;

    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String partitionedBy;

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    // 目标源中是否自动创建表，这样会方便不少
    public boolean autoCreateTable;


    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer shuffleParallelism;

    @FormField(ordinal = 12, type = FormFieldType.ENUM, validate = {Validator.require})
    public String batchOp;


    @FormField(ordinal = 100, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;


    @Override
    public IHiveConnGetter getHiveConnMeta() {
        return ParamsConfig.getItem(this.hiveConn, IHiveConnGetter.PLUGIN_NAME);
    }

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
        }

        public String getSourceTable(String tableName) {
            return tableName + IStreamTemplateData.KEY_STREAM_SOURCE_TABLE_SUFFIX;
        }

        public List<HdfsColMeta> getCols(String tableName) {
            HudiTableMeta tableMeta = getTableMeta(tableName);
            return tableMeta.colMetas;
        }

        public StringBuffer getSinkFlinkTableDDL(String tableName) {

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
                    appendColName(partitionedBy);
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
                        sub.appendLine("`" + partitionedBy + "`");
                    });
                    IHiveConnGetter hiveCfg = getHiveConnMeta();
                    if (StringUtils.isEmpty(dataXName)) {
                        throw new IllegalStateException("prop of dataXName can not be empty");
                    }
                    this.script.block(true, "WITH", (sub) -> {
                        sub.appendLine("'" + DataxUtils.DATAX_NAME + "' = '" + dataXName + "',");
                        sub.appendLine("'connector' = 'hudi',");
                        sub.appendLine("'path' = '" + tabMeta.getHudiDataDir(getFs().getFileSystem(), getHiveConnMeta()) + "',");
                        sub.appendLine("'table.type' = '" + tabMeta.getHudiTabType().getValue() + "',");

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
            //  }
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
                return "TIMESTAMP";
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

//    /**
//     * @return
//     */
//    public static List<Option> allPrimaryKeys(Object contextObj) {
//        List<Option> pks = Lists.newArrayList();
//        pks.add(new Option("base_id"));
//        return pks;
//    }

    @Override
    protected HudiDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
        return new HudiDataXContext(tableMap, this.dataXName);
    }


    public ISparkConnGetter getSparkConnGetter() {
        if (StringUtils.isEmpty(this.sparkConn)) {
            throw new IllegalArgumentException("param sparkConn can not be null");
        }
        return ISparkConnGetter.getConnGetter(this.sparkConn);
    }

    public Connection getConnection() {
        try {
            ParamsConfig connGetter = (ParamsConfig) getHiveConnMeta();
            return connGetter.createConfigInstance();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public String getTemplate() {
        return template;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHudiWriter.class, "DataXHudiWriter-tpl.json");
    }

//    @Override
//    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
//        return null;
//    }

    @TISExtension()
    public static class DefaultDescriptor extends DataXHdfsWriter.DefaultDescriptor implements IRewriteSuFormProperties {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME_SPARK_CONN, () -> ParamsConfig.getItems(ISparkConnGetter.PLUGIN_NAME));
            this.registerSelectOptions(BasicFSWriter.KEY_FIELD_NAME_HIVE_CONN, () -> ParamsConfig.getItems(IHiveConnGetter.PLUGIN_NAME));
        }

        public boolean validateTabType(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            try {
                HudiWriteTabType.parse(value);
            } catch (Throwable e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }

        public boolean validateBatchOp(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            try {
                BatchOpMode.parse(value);
            } catch (Throwable e) {
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }


        @Override
        public SuFormProperties overwriteSubPluginFormPropertyTypes(SuFormProperties subformProps) throws Exception {
            String overwriteSubField = IOUtils.loadResourceFromClasspath(DataXHudiWriter.class
                    , DataXHudiWriter.class.getSimpleName() + "." + subformProps.getSubFormFieldName() + ".json", true);
            JSONObject subField = JSON.parseObject(overwriteSubField);
            Class<?> clazz = DataXHudiWriter.class.getClassLoader().loadClass(subField.getString(SubForm.FIELD_DES_CLASS));
            return SuFormProperties.copy(filterFieldProp(Descriptor.buildPropertyTypes(this, clazz)), clazz, subformProps);
        }

        @Override
        public SuFormProperties.SuFormPropertiesBehaviorMeta overwriteBehaviorMeta(
                SuFormProperties.SuFormPropertiesBehaviorMeta behaviorMeta) throws Exception {


//            {
//                "clickBtnLabel": "设置",
//                    "onClickFillData": {
//                "cols": {
//                    "method": "getTableMetadata",
//                    "params": ["id"]
//                }
//            }
//            }

            Map<String, SuFormProperties.SuFormPropertyGetterMeta> onClickFillData = behaviorMeta.getOnClickFillData();

            SuFormProperties.SuFormPropertyGetterMeta propProcess = new SuFormProperties.SuFormPropertyGetterMeta();
            propProcess.setMethod(DataSourceMeta.METHOD_GET_PRIMARY_KEYS);
            propProcess.setParams(Collections.singletonList("id"));
            onClickFillData.put(HudiSelectedTab.KEY_RECORD_FIELD, propProcess);

            propProcess = new SuFormProperties.SuFormPropertyGetterMeta();
            propProcess.setMethod(DataSourceMeta.METHOD_GET_PARTITION_KEYS);
            propProcess.setParams(Collections.singletonList("id"));
            onClickFillData.put(HudiSelectedTab.KEY_PARTITION_PATH_FIELD, propProcess);
            onClickFillData.put(HudiSelectedTab.KEY_SOURCE_ORDERING_FIELD, propProcess);


            return behaviorMeta;
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        protected EndType getEndType() {
            return EndType.Hudi;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }

    public class HudiDataXContext extends FSDataXContext {

        private final HudiSelectedTab hudiTab;

        public HudiDataXContext(IDataxProcessor.TableMap tabMap, String dataxName) {
            super(tabMap, dataxName);
            ISelectedTab tab = tabMap.getSourceTab();
            if (!(tab instanceof HudiSelectedTab)) {
                throw new IllegalStateException(" param tabMap.getSourceTab() must be type of "
                        + HudiSelectedTab.class.getSimpleName() + " but now is :" + tab.getClass());
            }
            this.hudiTab = (HudiSelectedTab) tab;
        }

        public String getRecordField() {
            return this.hudiTab.recordField;
        }

        public String getPartitionPathField() {
            return this.hudiTab.partitionPathField;
        }

        public String getSourceOrderingField() {
            return this.hudiTab.sourceOrderingField;
        }

        public Integer getShuffleParallelism() {
            return shuffleParallelism;
        }

        public BatchOpMode getOpMode() {
            return BatchOpMode.parse(batchOp);
        }

        public HudiWriteTabType getTabType() {
            return HudiWriteTabType.parse(tabType);
        }
    }
}
