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
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.hive.IHiveConn;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.config.spark.ISparkConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicFSWriter;
import com.qlangtech.tis.plugin.datax.DataXHdfsWriter;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Collections;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-21 13:02
 **/
@Public
public class DataXHudiWriter extends BasicFSWriter implements KeyedPluginStore.IPluginKeyAware, IHiveConn {
    public static final String DATAX_NAME = "Hudi";

    public static final String KEY_FIELD_NAME_SPARK_CONN = "sparkConn";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String sparkConn;

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String hiveConn;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String tabType;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
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

//    @Override
//    public IHiveConnGetter getHiveConnMeta() {
//        return null;
//    }

    @Override
    public IHiveConnGetter getHiveConnMeta() {
        return ParamsConfig.getItem(this.hiveConn, IHiveConnGetter.PLUGIN_NAME);
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
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
